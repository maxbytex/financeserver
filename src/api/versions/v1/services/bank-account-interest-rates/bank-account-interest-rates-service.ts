import { inject, injectable } from "@needle-di/core";
import { and, asc, desc, eq, type SQL, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { DatabaseService } from "../../../../../core/services/database-service.ts";
import {
  bankAccountBalancesTable,
  bankAccountInterestRatesTable,
  bankAccountsTable,
} from "../../../../../db/schema.ts";
import { ServerError } from "../../models/server-error.ts";
import { decodeCursor } from "../../utils/cursor-utils.ts";
import { createOffsetPagination } from "../../utils/pagination-utils.ts";
import { toISOStringSafe } from "../../utils/date-utils.ts";
import {
  DEFAULT_PAGE_SIZE,
  MAX_PAGE_SIZE,
} from "../../constants/pagination-constants.ts";
import { SortOrder } from "../../enums/sort-order-enum.ts";
import type {
  CreateBankAccountInterestRateRequest,
  CreateBankAccountInterestRateResponse,
  GetBankAccountInterestRatesResponse,
  UpdateBankAccountInterestRateRequest,
  UpdateBankAccountInterestRateResponse,
} from "../../schemas/bank-account-interest-rates-schemas.ts";
import type { BankAccountInterestRateSummarySchema } from "../../schemas/bank-account-interest-rates-schemas.ts";
import { z } from "zod";
import { BankAccountCalculationsService } from "../bank-account-calculations/bank-account-calculations-service.ts";

type BankAccountInterestRateSummary = z.infer<
  typeof BankAccountInterestRateSummarySchema
>;

@injectable()
export class BankAccountInterestRatesService {
  constructor(
    private databaseService = inject(DatabaseService),
    private calculationsService = inject(BankAccountCalculationsService),
  ) {}

  public async createBankAccountInterestRate(
    payload: CreateBankAccountInterestRateRequest,
  ): Promise<CreateBankAccountInterestRateResponse> {
    const db = this.databaseService.get();
    const accountId = payload.bankAccountId;

    // Verify bank account exists
    const account = await db
      .select({ id: bankAccountsTable.id })
      .from(bankAccountsTable)
      .where(eq(bankAccountsTable.id, accountId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!account) {
      throw new ServerError(
        "BANK_ACCOUNT_NOT_FOUND",
        `Bank account with ID ${accountId} not found`,
        404,
      );
    }

    const response = await db.transaction(async (tx) => {
      // End any active interest rate for this bank account
      await this.endActiveInterestRate(
        tx,
        accountId,
        payload.interestRateStartDate,
      );

      // Validate interest rate period
      await this.validateNoOverlappingInterestRates(
        tx,
        accountId,
        payload.interestRateStartDate,
        payload.interestRateEndDate ?? "9999-12-31", // Treat null end date as far future for overlap check
        null,
      );

      const [result] = await tx
        .insert(bankAccountInterestRatesTable)
        .values({
          bankAccountId: accountId,
          interestRate: payload.interestRate.toString(),
          interestRateStartDate: payload.interestRateStartDate,
          interestRateEndDate: payload.interestRateEndDate ?? null,
        })
        .returning();

      return this.mapInterestRateToResponse(result);
    });

    await this.triggerInterestCalculationForAccount(accountId);

    return response;
  }

  public async getBankAccountInterestRates(payload: {
    bankAccountId?: number;
    limit?: number;
    cursor?: string;
    sortOrder?: SortOrder;
  }): Promise<GetBankAccountInterestRatesResponse> {
    const db = this.databaseService.get();
    const accountId = payload.bankAccountId;
    const pageSize = payload.limit ?? DEFAULT_PAGE_SIZE;
    const cursor = payload.cursor;
    const sortOrder = payload.sortOrder ?? SortOrder.Desc;

    // Verify bank account exists if provided
    if (accountId) {
      const account = await db
        .select({ id: bankAccountsTable.id })
        .from(bankAccountsTable)
        .where(eq(bankAccountsTable.id, accountId))
        .limit(1)
        .then((rows) => rows[0]);

      if (!account) {
        throw new ServerError(
          "BANK_ACCOUNT_NOT_FOUND",
          `Bank account with ID ${accountId} not found`,
          404,
        );
      }
    }

    const size = Math.min(pageSize, MAX_PAGE_SIZE);
    const offset = cursor ? decodeCursor(cursor) : 0;

    const orderDirection = sortOrder === SortOrder.Asc ? asc : desc;

    const conditions: SQL[] = [];
    if (accountId) {
      conditions.push(
        eq(bankAccountInterestRatesTable.bankAccountId, accountId),
      );
    }

    const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

    const [{ count }] = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(bankAccountInterestRatesTable)
      .where(whereClause);

    const total = Number(count ?? 0);

    if (total === 0) {
      return {
        results: [],
        limit: size,
        offset: offset,
        total: 0,
        nextCursor: null,
        previousCursor: null,
      };
    }

    const results = await db
      .select()
      .from(bankAccountInterestRatesTable)
      .where(whereClause)
      .orderBy(
        orderDirection(bankAccountInterestRatesTable.createdAt),
        orderDirection(bankAccountInterestRatesTable.id),
      )
      .limit(size)
      .offset(offset);

    const data: BankAccountInterestRateSummary[] = results.map((rate) =>
      this.mapInterestRateToSummary(rate)
    );

    const pagination = createOffsetPagination<BankAccountInterestRateSummary>(
      data,
      size,
      offset,
      total,
    );

    return {
      results: pagination.results,
      limit: pagination.limit,
      offset: pagination.offset,
      total: pagination.total,
      nextCursor: pagination.nextCursor,
      previousCursor: pagination.previousCursor,
    };
  }

  public async updateBankAccountInterestRate(
    rateId: number,
    payload: UpdateBankAccountInterestRateRequest,
  ): Promise<UpdateBankAccountInterestRateResponse> {
    const db = this.databaseService.get();

    // Verify rate exists
    const existingRate = await db
      .select()
      .from(bankAccountInterestRatesTable)
      .where(eq(bankAccountInterestRatesTable.id, rateId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existingRate) {
      throw new ServerError(
        "INTEREST_RATE_NOT_FOUND",
        `Interest rate with ID ${rateId} not found`,
        404,
      );
    }

    const accountId = existingRate.bankAccountId;

    const updateValues: {
      interestRate?: string;
      interestRateStartDate?: string;
      interestRateEndDate?: string | null;
      updatedAt: Date;
    } = {
      updatedAt: new Date(),
    };

    if (payload.interestRate !== undefined) {
      updateValues.interestRate = payload.interestRate.toString();
    }

    if (payload.interestRateStartDate !== undefined) {
      updateValues.interestRateStartDate = payload.interestRateStartDate;
    }

    if (payload.interestRateEndDate !== undefined) {
      updateValues.interestRateEndDate = payload.interestRateEndDate;
    }

    const response = await db.transaction(async (tx) => {
      // Validate interest rate period if being updated
      const newStartDate = payload.interestRateStartDate ??
        existingRate.interestRateStartDate;
      const newEndDate = payload.interestRateEndDate !== undefined
        ? payload.interestRateEndDate
        : existingRate.interestRateEndDate;

      // Treat null end date as far future
      const effectiveEndDate = newEndDate === null ? "9999-12-31" : newEndDate;

      if (
        payload.interestRateStartDate !== undefined ||
        payload.interestRateEndDate !== undefined
      ) {
        await this.validateNoOverlappingInterestRates(
          tx,
          accountId,
          newStartDate,
          effectiveEndDate,
          rateId,
        );
      }

      const [result] = await tx
        .update(bankAccountInterestRatesTable)
        .set(updateValues)
        .where(eq(bankAccountInterestRatesTable.id, rateId))
        .returning();

      return this.mapInterestRateToResponse(result);
    });

    await this.triggerInterestCalculationForAccount(accountId);

    return response;
  }

  public async deleteBankAccountInterestRate(rateId: number): Promise<void> {
    const db = this.databaseService.get();

    const [existing] = await db
      .select({ bankAccountId: bankAccountInterestRatesTable.bankAccountId })
      .from(bankAccountInterestRatesTable)
      .where(eq(bankAccountInterestRatesTable.id, rateId))
      .limit(1);

    if (!existing) {
      throw new ServerError(
        "INTEREST_RATE_NOT_FOUND",
        `Interest rate with ID ${rateId} not found`,
        404,
      );
    }

    const result = await db
      .delete(bankAccountInterestRatesTable)
      .where(eq(bankAccountInterestRatesTable.id, rateId))
      .returning({ id: bankAccountInterestRatesTable.id });

    if (result.length === 0) {
      throw new ServerError(
        "INTEREST_RATE_NOT_FOUND",
        `Interest rate with ID ${rateId} not found`,
        404,
      );
    }

    await this.triggerInterestCalculationForAccount(existing.bankAccountId);
  }

  private async validateNoOverlappingInterestRates(
    db:
      | NodePgDatabase<Record<string, never>>
      | Parameters<
        Parameters<NodePgDatabase<Record<string, never>>["transaction"]>[0]
      >[0],
    bankAccountId: number,
    startDate: string,
    endDate: string,
    excludeRateId: number | null,
  ): Promise<void> {
    // Check if the new period overlaps with any existing periods
    const conditions: SQL[] = [
      eq(bankAccountInterestRatesTable.bankAccountId, bankAccountId),
      // Overlap condition:
      // new_start <= existing_end (or infinity) AND new_end (or infinity) >= existing_start

      // We need to handle null end dates in DB as infinity
      // Using COALESCE is one way, or logic

      // Complex overlap logic in SQL:
      // (start1 <= end2) and (end1 >= start2)
      // where end can be null (infinity)

      sql`
        (${startDate} <= COALESCE(${bankAccountInterestRatesTable.interestRateEndDate}, '9999-12-31'))
        AND
        (${endDate} >= ${bankAccountInterestRatesTable.interestRateStartDate})
      `,
    ];

    if (excludeRateId !== null) {
      conditions.push(
        sql`${bankAccountInterestRatesTable.id} != ${excludeRateId}`,
      );
    }

    const overlapping = await db
      .select({
        id: bankAccountInterestRatesTable.id,
        startDate: bankAccountInterestRatesTable.interestRateStartDate,
        endDate: bankAccountInterestRatesTable.interestRateEndDate,
      })
      .from(bankAccountInterestRatesTable)
      .where(and(...conditions))
      .limit(1);

    if (overlapping.length > 0) {
      const existing = overlapping[0];
      throw new ServerError(
        "OVERLAPPING_INTEREST_RATE_PERIOD",
        `Interest rate period ${startDate} to ${endDate} overlaps with existing period ${existing.startDate} to ${
          existing.endDate ?? "ongoing"
        }`,
        400,
      );
    }
  }

  private async endActiveInterestRate(
    db:
      | NodePgDatabase<Record<string, never>>
      | Parameters<
        Parameters<NodePgDatabase<Record<string, never>>["transaction"]>[0]
      >[0],
    bankAccountId: number,
    newRateStartDate: string,
  ): Promise<void> {
    const endDate = new Date(newRateStartDate);
    endDate.setUTCDate(endDate.getUTCDate() - 1);
    const oldRateEndDate = endDate.toISOString().split("T")[0];

    await db
      .update(bankAccountInterestRatesTable)
      .set({
        interestRateEndDate: oldRateEndDate,
        updatedAt: new Date(),
      })
      .where(
        and(
          eq(bankAccountInterestRatesTable.bankAccountId, bankAccountId),
          sql`${bankAccountInterestRatesTable.interestRateStartDate} < ${newRateStartDate}`,
          sql`(${bankAccountInterestRatesTable.interestRateEndDate} IS NULL 
               OR ${bankAccountInterestRatesTable.interestRateEndDate} >= ${newRateStartDate})`,
        ),
      );
  }

  private mapInterestRateToResponse(
    rate: typeof bankAccountInterestRatesTable.$inferSelect,
  ): CreateBankAccountInterestRateResponse {
    return {
      id: rate.id,
      bankAccountId: rate.bankAccountId,
      interestRate: parseFloat(rate.interestRate),
      interestRateStartDate: rate.interestRateStartDate,
      interestRateEndDate: rate.interestRateEndDate,
      createdAt: toISOStringSafe(rate.createdAt),
      updatedAt: toISOStringSafe(rate.updatedAt),
    };
  }

  private mapInterestRateToSummary(
    rate: typeof bankAccountInterestRatesTable.$inferSelect,
  ): BankAccountInterestRateSummary {
    return {
      id: rate.id,
      bankAccountId: rate.bankAccountId,
      interestRate: parseFloat(rate.interestRate),
      interestRateStartDate: rate.interestRateStartDate,
      interestRateEndDate: rate.interestRateEndDate,
      createdAt: toISOStringSafe(rate.createdAt),
      updatedAt: toISOStringSafe(rate.updatedAt),
    };
  }

  /**
   * Calculate interest profit after tax for a bank account
   * @param bankAccountId - Bank account ID
   * @param currentBalance - Current balance as string
   * @param currencyCode - Currency code for the balance
   * @returns Calculated monthly and annual profit after tax, or null if no active rate
   */
  public async calculateInterestAfterTax(
    bankAccountId: number,
    currentBalance: string,
    currencyCode: string,
  ): Promise<
    {
      monthlyProfit: string;
      annualProfit: string;
      currencyCode: string;
    } | null
  > {
    try {
      const db = this.databaseService.get();

      // Get current active interest rate
      const now = new Date().toISOString().split("T")[0];
      const [activeRate] = await db
        .select({
          interestRate: bankAccountInterestRatesTable.interestRate,
          taxPercentage: bankAccountsTable.taxPercentage,
        })
        .from(bankAccountInterestRatesTable)
        .innerJoin(
          bankAccountsTable,
          eq(bankAccountsTable.id, bankAccountInterestRatesTable.bankAccountId),
        )
        .where(
          and(
            eq(bankAccountInterestRatesTable.bankAccountId, bankAccountId),
            sql`${bankAccountInterestRatesTable.interestRateStartDate} <= ${now}`,
            sql`(${bankAccountInterestRatesTable.interestRateEndDate} IS NULL OR ${bankAccountInterestRatesTable.interestRateEndDate} >= ${now})`,
          ),
        )
        .orderBy(
          desc(bankAccountInterestRatesTable.interestRateStartDate),
          desc(bankAccountInterestRatesTable.createdAt),
        )
        .limit(1);

      if (!activeRate) {
        console.warn(
          `No active interest rate found for bank account ${bankAccountId}`,
        );
        return null;
      }

      const balance = parseFloat(currentBalance);
      const interestRate = parseFloat(activeRate.interestRate);
      const taxPercentage = activeRate.taxPercentage
        ? parseFloat(activeRate.taxPercentage)
        : 0;

      // Calculate annual profit before tax
      const annualProfitBeforeTax = balance * interestRate;

      // Calculate monthly profit before tax
      const monthlyProfitBeforeTax = annualProfitBeforeTax / 12;

      // Apply tax
      const taxMultiplier = 1 - taxPercentage;
      const monthlyProfit = monthlyProfitBeforeTax * taxMultiplier;
      const annualProfit = annualProfitBeforeTax * taxMultiplier;

      // Store calculation
      await this.calculationsService.storeCalculation(
        bankAccountId,
        monthlyProfit.toFixed(2),
        annualProfit.toFixed(2),
        currencyCode,
      );

      return {
        monthlyProfit: monthlyProfit.toFixed(2),
        annualProfit: annualProfit.toFixed(2),
        currencyCode,
      };
    } catch (error) {
      console.error("Error calculating interest after tax:", error);
      // Rethrow so callers can handle persistence/calculation failures
      throw error;
    }
  }

  /**
   * Calculate after-tax interest for all bank accounts with active interest rates
   */
  public async calculateAllBankAccountInterestRates(): Promise<void> {
    const db = this.databaseService.get();

    // Get all bank accounts with their latest balances using a subquery
    const latestBalances = db.$with("latest_balances").as(
      db
        .select({
          bankAccountId: bankAccountBalancesTable.bankAccountId,
          balance: bankAccountBalancesTable.balance,
          currencyCode: bankAccountBalancesTable.currencyCode,
          createdAt: bankAccountBalancesTable.createdAt,
          id: bankAccountBalancesTable.id,
          rowNumber: sql<
            number
          >`ROW_NUMBER() OVER (PARTITION BY ${bankAccountBalancesTable.bankAccountId} ORDER BY ${bankAccountBalancesTable.createdAt} DESC, ${bankAccountBalancesTable.id} DESC)`
            .as(
              "row_number",
            ),
        })
        .from(bankAccountBalancesTable),
    );

    const bankAccountsWithBalances = await db
      .with(latestBalances)
      .select({
        bankAccountId: latestBalances.bankAccountId,
        balance: latestBalances.balance,
        currencyCode: latestBalances.currencyCode,
      })
      .from(latestBalances)
      .where(eq(latestBalances.rowNumber, 1));

    console.log(
      `Processing ${bankAccountsWithBalances.length} bank accounts with interest rates`,
    );

    // Calculate interest after tax for each account
    for (const account of bankAccountsWithBalances) {
      try {
        await this.calculateInterestAfterTax(
          account.bankAccountId,
          account.balance,
          account.currencyCode,
        );
      } catch (error) {
        console.error(
          `Failed to calculate interest for bank account ${account.bankAccountId}:`,
          error,
        );
      }
    }
  }

  public async triggerInterestCalculationForAccount(
    bankAccountId: number,
  ): Promise<void> {
    const db = this.databaseService.get();

    // Get the latest balance for the account
    const [latestBalance] = await db
      .select({
        balance: bankAccountBalancesTable.balance,
        currencyCode: bankAccountBalancesTable.currencyCode,
      })
      .from(bankAccountBalancesTable)
      .where(eq(bankAccountBalancesTable.bankAccountId, bankAccountId))
      .orderBy(desc(bankAccountBalancesTable.createdAt))
      .limit(1);

    if (latestBalance) {
      await this.calculateInterestAfterTax(
        bankAccountId,
        latestBalance.balance,
        latestBalance.currencyCode,
      );
    }
  }
}
