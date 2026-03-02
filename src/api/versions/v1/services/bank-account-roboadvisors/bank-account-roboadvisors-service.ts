import { inject, injectable } from "@needle-di/core";
import {
  asc,
  desc,
  eq,
  getTableColumns,
  ilike,
  type SQL,
  sql,
} from "drizzle-orm";
import { DatabaseService } from "../../../../../core/services/database-service.ts";
import {
  bankAccountsTable,
  roboadvisorBalances,
  roboadvisorFundCalculationsTable,
  roboadvisorFunds,
  roboadvisors,
} from "../../../../../db/schema.ts";
import { ServerError } from "../../models/server-error.ts";
import { decodeCursor } from "../../utils/cursor-utils.ts";
import { createOffsetPagination } from "../../utils/pagination-utils.ts";
import { buildAndFilters } from "../../utils/sql-utils.ts";
import { toISOStringSafe } from "../../utils/date-utils.ts";
import {
  DEFAULT_PAGE_SIZE,
  MAX_PAGE_SIZE,
} from "../../constants/pagination-constants.ts";
import { SortOrder } from "../../enums/sort-order-enum.ts";
import { BankAccountRoboadvisorSortField } from "../../enums/bank-account-roboadvisor-sort-field-enum.ts";
import { BankAccountRoboadvisorBalanceSortField } from "../../enums/bank-account-roboadvisor-balance-sort-field-enum.ts";
import { BankAccountRoboadvisorFundSortField } from "../../enums/bank-account-roboadvisor-fund-sort-field-enum.ts";
import { BankAccountRoboadvisorsFilter } from "../../interfaces/bank-account-roboadvisors/bank-account-roboadvisors-filter-interface.ts";
import { BankAccountRoboadvisorBalancesFilter } from "../../interfaces/bank-account-roboadvisors/bank-account-roboadvisor-balances-filter-interface.ts";
import { BankAccountRoboadvisorFundsFilter } from "../../interfaces/bank-account-roboadvisors/bank-account-roboadvisor-funds-filter-interface.ts";
import { BankAccountRoboadvisorSummary } from "../../interfaces/bank-account-roboadvisors/bank-account-roboadvisor-summary-interface.ts";
import { BankAccountRoboadvisorBalanceSummary } from "../../interfaces/bank-account-roboadvisors/bank-account-roboadvisor-balance-summary-interface.ts";
import { BankAccountRoboadvisorFundSummary } from "../../interfaces/bank-account-roboadvisors/bank-account-roboadvisor-fund-summary-interface.ts";
import type {
  CreateBankAccountRoboadvisorRequest,
  CreateBankAccountRoboadvisorResponse,
  GetBankAccountRoboadvisorsResponse,
  UpdateBankAccountRoboadvisorRequest,
  UpdateBankAccountRoboadvisorResponse,
} from "../../schemas/bank-account-roboadvisors-schemas.ts";
import type {
  CreateBankAccountRoboadvisorBalanceRequest,
  CreateBankAccountRoboadvisorBalanceResponse,
  GetBankAccountRoboadvisorBalancesResponse,
  UpdateBankAccountRoboadvisorBalanceRequest,
  UpdateBankAccountRoboadvisorBalanceResponse,
} from "../../schemas/bank-account-roboadvisor-balances-schemas.ts";
import type {
  CreateBankAccountRoboadvisorFundRequest,
  CreateBankAccountRoboadvisorFundResponse,
  GetBankAccountRoboadvisorFundsResponse,
  UpdateBankAccountRoboadvisorFundRequest,
  UpdateBankAccountRoboadvisorFundResponse,
} from "../../schemas/bank-account-roboadvisor-funds-schemas.ts";
import { BankAccountRoboadvisorFundCalculationsService } from "../bank-account-roboadvisor-fund-calculations/bank-account-roboadvisor-fund-calculations-service.ts";
import { IndexFundPriceProviderFactory } from "../external-pricing/factory/index-fund-price-provider-factory.ts";

@injectable()
export class BankAccountRoboadvisorsService {
  constructor(
    private databaseService = inject(DatabaseService),
    private calculationsService = inject(
      BankAccountRoboadvisorFundCalculationsService,
    ),
    private priceProviderFactory = inject(IndexFundPriceProviderFactory),
  ) {}

  // Roboadvisor CRUD operations
  public async createBankAccountRoboadvisor(
    payload: CreateBankAccountRoboadvisorRequest,
  ): Promise<CreateBankAccountRoboadvisorResponse> {
    const db = this.databaseService.get();

    // Verify bank account exists
    const account = await db
      .select({ id: bankAccountsTable.id })
      .from(bankAccountsTable)
      .where(eq(bankAccountsTable.id, payload.bankAccountId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!account) {
      throw new ServerError(
        "BANK_ACCOUNT_NOT_FOUND",
        `Bank account with ID ${payload.bankAccountId} not found`,
        404,
      );
    }

    const [result] = await db
      .insert(roboadvisors)
      .values({
        name: payload.name,
        bankAccountId: payload.bankAccountId,
        riskLevel: payload.riskLevel ?? null,
        managementFeePercentage: payload.managementFeePercentage.toString(),
        custodyFeePercentage: payload.custodyFeePercentage.toString(),
        fundTerPercentage: payload.fundTerPercentage.toString(),
        totalFeePercentage: payload.totalFeePercentage.toString(),
        managementFeeFrequency: payload.managementFeeFrequency,
        custodyFeeFrequency: payload.custodyFeeFrequency,
        terPricedInNav: payload.terPricedInNav ?? true,
        taxPercentage:
          payload.taxPercentage === null || payload.taxPercentage === undefined
            ? null
            : payload.taxPercentage.toString(),
      })
      .returning();

    return this.mapRoboadvisorToResponse(result);
  }

  public async getBankAccountRoboadvisors(
    filter: BankAccountRoboadvisorsFilter,
  ): Promise<GetBankAccountRoboadvisorsResponse> {
    const db = this.databaseService.get();

    const pageSize = Math.min(
      filter.pageSize ?? DEFAULT_PAGE_SIZE,
      MAX_PAGE_SIZE,
    );
    const offset = filter.cursor ? decodeCursor(filter.cursor) : 0;
    const sortField = filter.sortField ??
      BankAccountRoboadvisorSortField.CreatedAt;
    const sortOrder = filter.sortOrder ?? SortOrder.Desc;

    const conditions: SQL[] = [];

    if (filter.bankAccountId !== undefined) {
      conditions.push(eq(roboadvisors.bankAccountId, filter.bankAccountId));
    }

    if (filter.name) {
      conditions.push(ilike(roboadvisors.name, `%${filter.name}%`));
    }

    const whereClause = conditions.length > 0
      ? buildAndFilters(conditions)
      : undefined;

    const orderColumn = this.getRoboadvisorSortColumn(sortField);
    const orderDirection = sortOrder === SortOrder.Asc ? asc : desc;

    const [{ count }] = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(roboadvisors)
      .where(whereClause);

    const total = Number(count ?? 0);

    if (total === 0) {
      return {
        results: [],
        limit: pageSize,
        offset: offset,
        total: 0,
        nextCursor: null,
        previousCursor: null,
      };
    }

    const results = await db
      .select({
        ...getTableColumns(roboadvisors),
        latestCalculation: sql<
          {
            currentValue: string;
            currencyCode: string;
            calculatedAt: string;
          } | null
        >`(
          SELECT json_build_object(
            'currentValue', fund_calculation.current_value,
            'currencyCode', latest_balance.currency_code,
            'calculatedAt', fund_calculation.created_at
          )
          FROM ${roboadvisorFundCalculationsTable} fund_calculation
          LEFT JOIN LATERAL (
            SELECT currency_code
            FROM ${roboadvisorBalances} roboadvisor_balance
            WHERE roboadvisor_balance.roboadvisor_id = ${roboadvisors}.id
            ORDER BY roboadvisor_balance.date DESC
            LIMIT 1
          ) latest_balance ON true
          WHERE fund_calculation.roboadvisor_id = ${roboadvisors}.id
          ORDER BY fund_calculation.created_at DESC
          LIMIT 1
        )`,
      })
      .from(roboadvisors)
      .where(whereClause)
      .orderBy(orderDirection(orderColumn), orderDirection(roboadvisors.id))
      .limit(pageSize)
      .offset(offset);

    const data: BankAccountRoboadvisorSummary[] = results.map((roboadvisor) =>
      this.mapRoboadvisorToSummary(roboadvisor)
    );

    const pagination = createOffsetPagination<BankAccountRoboadvisorSummary>(
      data,
      pageSize,
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

  public async updateBankAccountRoboadvisor(
    roboadvisorId: number,
    payload: UpdateBankAccountRoboadvisorRequest,
  ): Promise<UpdateBankAccountRoboadvisorResponse> {
    const db = this.databaseService.get();

    // Verify roboadvisor exists
    const existing = await db
      .select()
      .from(roboadvisors)
      .where(eq(roboadvisors.id, roboadvisorId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existing) {
      throw new ServerError(
        "ROBOADVISOR_NOT_FOUND",
        `Roboadvisor with ID ${roboadvisorId} not found`,
        404,
      );
    }

    const updateValues: Record<string, unknown> = {
      updatedAt: new Date(),
    };

    if (payload.name !== undefined) updateValues.name = payload.name;
    if (payload.riskLevel !== undefined) {
      updateValues.riskLevel = payload.riskLevel;
    }
    if (payload.managementFeePercentage !== undefined) {
      updateValues.managementFeePercentage = payload.managementFeePercentage
        .toString();
    }
    if (payload.custodyFeePercentage !== undefined) {
      updateValues.custodyFeePercentage = payload.custodyFeePercentage
        .toString();
    }
    if (payload.fundTerPercentage !== undefined) {
      updateValues.fundTerPercentage = payload.fundTerPercentage.toString();
    }
    if (payload.totalFeePercentage !== undefined) {
      updateValues.totalFeePercentage = payload.totalFeePercentage.toString();
    }
    if (payload.managementFeeFrequency !== undefined) {
      updateValues.managementFeeFrequency = payload.managementFeeFrequency;
    }
    if (payload.custodyFeeFrequency !== undefined) {
      updateValues.custodyFeeFrequency = payload.custodyFeeFrequency;
    }
    if (payload.terPricedInNav !== undefined) {
      updateValues.terPricedInNav = payload.terPricedInNav;
    }
    if (payload.taxPercentage !== undefined) {
      updateValues.taxPercentage = payload.taxPercentage === null
        ? null
        : payload.taxPercentage.toString();
    }

    const [result] = await db
      .update(roboadvisors)
      .set(updateValues)
      .where(eq(roboadvisors.id, roboadvisorId))
      .returning();

    if (!result) {
      throw new ServerError(
        "ROBOADVISOR_NOT_FOUND",
        `Roboadvisor with ID ${roboadvisorId} not found`,
        404,
      );
    }

    // Trigger recalculation if relevant fields changed
    if (
      payload.managementFeePercentage !== undefined ||
      payload.custodyFeePercentage !== undefined ||
      payload.fundTerPercentage !== undefined ||
      payload.totalFeePercentage !== undefined ||
      payload.taxPercentage !== undefined
    ) {
      this.calculateRoboadvisorValueAfterTax(roboadvisorId).catch((error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${roboadvisorId}:`,
          error,
        );
      });
    }

    const latestCalculation = await this.getLatestCalculation(roboadvisorId);

    return this.mapRoboadvisorToSummary({
      ...result,
      latestCalculation,
    });
  }

  private async getLatestCalculation(roboadvisorId: number): Promise<
    {
      currentValue: string;
      currencyCode: string;
      calculatedAt: string;
    } | null
  > {
    const db = this.databaseService.get();

    const [calculation] = await db
      .select({
        currentValue: roboadvisorFundCalculationsTable.currentValue,
        calculatedAt: roboadvisorFundCalculationsTable.createdAt,
      })
      .from(roboadvisorFundCalculationsTable)
      .where(eq(roboadvisorFundCalculationsTable.roboadvisorId, roboadvisorId))
      .orderBy(desc(roboadvisorFundCalculationsTable.createdAt))
      .limit(1);

    if (!calculation) {
      return null;
    }

    const [latestBalance] = await db
      .select({
        currencyCode: roboadvisorBalances.currencyCode,
      })
      .from(roboadvisorBalances)
      .where(eq(roboadvisorBalances.roboadvisorId, roboadvisorId))
      .orderBy(desc(roboadvisorBalances.date))
      .limit(1);

    if (!latestBalance?.currencyCode) {
      return null;
    }

    return {
      currentValue: calculation.currentValue,
      currencyCode: latestBalance.currencyCode,
      calculatedAt: calculation.calculatedAt.toISOString(),
    };
  }

  public async deleteBankAccountRoboadvisor(
    roboadvisorId: number,
  ): Promise<void> {
    const db = this.databaseService.get();

    const result = await db
      .delete(roboadvisors)
      .where(eq(roboadvisors.id, roboadvisorId))
      .returning({ id: roboadvisors.id });

    if (result.length === 0) {
      throw new ServerError(
        "ROBOADVISOR_NOT_FOUND",
        `Roboadvisor with ID ${roboadvisorId} not found`,
        404,
      );
    }
  }

  // Roboadvisor Balance CRUD operations
  public async createBankAccountRoboadvisorBalance(
    payload: CreateBankAccountRoboadvisorBalanceRequest,
  ): Promise<CreateBankAccountRoboadvisorBalanceResponse> {
    const db = this.databaseService.get();

    // Verify roboadvisor exists
    const roboadvisor = await db
      .select({ id: roboadvisors.id })
      .from(roboadvisors)
      .where(eq(roboadvisors.id, payload.roboadvisorId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!roboadvisor) {
      throw new ServerError(
        "ROBOADVISOR_NOT_FOUND",
        `Roboadvisor with ID ${payload.roboadvisorId} not found`,
        404,
      );
    }

    const [result] = await db
      .insert(roboadvisorBalances)
      .values({
        roboadvisorId: payload.roboadvisorId,
        date: payload.date,
        type: payload.type,
        amount: payload.amount,
        currencyCode: payload.currencyCode,
      })
      .returning();

    this.calculateRoboadvisorValueAfterTax(payload.roboadvisorId).catch(
      (error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${payload.roboadvisorId}:`,
          error,
        );
      },
    );

    return this.mapBalanceToResponse(result);
  }

  public async getBankAccountRoboadvisorBalances(
    filter: BankAccountRoboadvisorBalancesFilter,
  ): Promise<GetBankAccountRoboadvisorBalancesResponse> {
    const db = this.databaseService.get();

    const pageSize = Math.min(
      filter.pageSize ?? DEFAULT_PAGE_SIZE,
      MAX_PAGE_SIZE,
    );
    const offset = filter.cursor ? decodeCursor(filter.cursor) : 0;
    const sortField = filter.sortField ??
      BankAccountRoboadvisorBalanceSortField.Date;
    const sortOrder = filter.sortOrder ?? SortOrder.Desc;

    const conditions: SQL[] = [];

    if (filter.roboadvisorId !== undefined) {
      conditions.push(
        eq(roboadvisorBalances.roboadvisorId, filter.roboadvisorId),
      );
    }

    const whereClause = conditions.length > 0
      ? buildAndFilters(conditions)
      : undefined;

    const orderColumn = this.getBalanceSortColumn(sortField);
    const orderDirection = sortOrder === SortOrder.Asc ? asc : desc;

    const [{ count }] = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(roboadvisorBalances)
      .where(whereClause);

    const total = Number(count ?? 0);

    if (total === 0) {
      return {
        results: [],
        limit: pageSize,
        offset: offset,
        total: 0,
        nextCursor: null,
        previousCursor: null,
      };
    }

    const results = await db
      .select()
      .from(roboadvisorBalances)
      .where(whereClause)
      .orderBy(
        orderDirection(orderColumn),
        orderDirection(roboadvisorBalances.id),
      )
      .limit(pageSize)
      .offset(offset);

    const data: BankAccountRoboadvisorBalanceSummary[] = results.map(
      (balance) => this.mapBalanceToSummary(balance),
    );

    const pagination = createOffsetPagination<
      BankAccountRoboadvisorBalanceSummary
    >(
      data,
      pageSize,
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

  public async updateBankAccountRoboadvisorBalance(
    balanceId: number,
    payload: UpdateBankAccountRoboadvisorBalanceRequest,
  ): Promise<UpdateBankAccountRoboadvisorBalanceResponse> {
    const db = this.databaseService.get();

    // Verify balance exists
    const existing = await db
      .select()
      .from(roboadvisorBalances)
      .where(eq(roboadvisorBalances.id, balanceId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existing) {
      throw new ServerError(
        "ROBOADVISOR_BALANCE_NOT_FOUND",
        `Roboadvisor balance with ID ${balanceId} not found`,
        404,
      );
    }

    const updateValues: Record<string, unknown> = {
      updatedAt: new Date(),
    };

    if (payload.date !== undefined) updateValues.date = payload.date;
    if (payload.type !== undefined) updateValues.type = payload.type;
    if (payload.amount !== undefined) updateValues.amount = payload.amount;
    if (payload.currencyCode !== undefined) {
      updateValues.currencyCode = payload.currencyCode;
    }

    const [result] = await db
      .update(roboadvisorBalances)
      .set(updateValues)
      .where(eq(roboadvisorBalances.id, balanceId))
      .returning();

    this.calculateRoboadvisorValueAfterTax(existing.roboadvisorId).catch(
      (error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${existing.roboadvisorId}:`,
          error,
        );
      },
    );

    return this.mapBalanceToResponse(result);
  }

  public async deleteBankAccountRoboadvisorBalance(
    balanceId: number,
  ): Promise<void> {
    const db = this.databaseService.get();

    const existing = await db
      .select({ roboadvisorId: roboadvisorBalances.roboadvisorId })
      .from(roboadvisorBalances)
      .where(eq(roboadvisorBalances.id, balanceId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existing) {
      throw new ServerError(
        "ROBOADVISOR_BALANCE_NOT_FOUND",
        `Roboadvisor balance with ID ${balanceId} not found`,
        404,
      );
    }

    const result = await db
      .delete(roboadvisorBalances)
      .where(eq(roboadvisorBalances.id, balanceId))
      .returning({ id: roboadvisorBalances.id });

    if (result.length === 0) {
      throw new ServerError(
        "ROBOADVISOR_BALANCE_NOT_FOUND",
        `Roboadvisor balance with ID ${balanceId} not found`,
        404,
      );
    }

    this.calculateRoboadvisorValueAfterTax(existing.roboadvisorId).catch(
      (error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${existing.roboadvisorId}:`,
          error,
        );
      },
    );
  }

  // Roboadvisor Fund CRUD operations
  public async createBankAccountRoboadvisorFund(
    payload: CreateBankAccountRoboadvisorFundRequest,
  ): Promise<CreateBankAccountRoboadvisorFundResponse> {
    const db = this.databaseService.get();

    // Verify roboadvisor exists
    const roboadvisor = await db
      .select({ id: roboadvisors.id })
      .from(roboadvisors)
      .where(eq(roboadvisors.id, payload.roboadvisorId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!roboadvisor) {
      throw new ServerError(
        "ROBOADVISOR_NOT_FOUND",
        `Roboadvisor with ID ${payload.roboadvisorId} not found`,
        404,
      );
    }

    const [result] = await db
      .insert(roboadvisorFunds)
      .values({
        roboadvisorId: payload.roboadvisorId,
        name: payload.name,
        isin: payload.isin,
        assetClass: payload.assetClass,
        region: payload.region,
        fundCurrencyCode: payload.fundCurrencyCode,
        weight: payload.weight.toString(),
        shareCount: payload.shareCount ? payload.shareCount.toString() : null,
      })
      .returning();

    this.calculateRoboadvisorValueAfterTax(payload.roboadvisorId).catch(
      (error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${payload.roboadvisorId}:`,
          error,
        );
      },
    );

    return this.mapFundToResponse(result);
  }

  public async getBankAccountRoboadvisorFunds(
    filter: BankAccountRoboadvisorFundsFilter,
  ): Promise<GetBankAccountRoboadvisorFundsResponse> {
    const db = this.databaseService.get();

    const pageSize = Math.min(
      filter.pageSize ?? DEFAULT_PAGE_SIZE,
      MAX_PAGE_SIZE,
    );
    const offset = filter.cursor ? decodeCursor(filter.cursor) : 0;
    const sortField = filter.sortField ??
      BankAccountRoboadvisorFundSortField.Name;
    const sortOrder = filter.sortOrder ?? SortOrder.Asc;

    const conditions: SQL[] = [];

    if (filter.roboadvisorId !== undefined) {
      conditions.push(eq(roboadvisorFunds.roboadvisorId, filter.roboadvisorId));
    }

    if (filter.name) {
      conditions.push(ilike(roboadvisorFunds.name, `%${filter.name}%`));
    }

    if (filter.isin) {
      conditions.push(ilike(roboadvisorFunds.isin, `%${filter.isin}%`));
    }

    if (filter.assetClass) {
      conditions.push(
        ilike(roboadvisorFunds.assetClass, `%${filter.assetClass}%`),
      );
    }

    if (filter.region) {
      conditions.push(ilike(roboadvisorFunds.region, `%${filter.region}%`));
    }

    if (filter.fundCurrencyCode) {
      conditions.push(
        eq(roboadvisorFunds.fundCurrencyCode, filter.fundCurrencyCode),
      );
    }

    const whereClause = conditions.length > 0
      ? buildAndFilters(conditions)
      : undefined;

    const orderColumn = this.getFundSortColumn(sortField);
    const orderDirection = sortOrder === SortOrder.Asc ? asc : desc;

    const [{ count }] = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(roboadvisorFunds)
      .where(whereClause);

    const total = Number(count ?? 0);

    if (total === 0) {
      return {
        results: [],
        limit: pageSize,
        offset: offset,
        total: 0,
        nextCursor: null,
        previousCursor: null,
      };
    }

    const results = await db
      .select()
      .from(roboadvisorFunds)
      .where(whereClause)
      .orderBy(orderDirection(orderColumn), orderDirection(roboadvisorFunds.id))
      .limit(pageSize)
      .offset(offset);

    const data: BankAccountRoboadvisorFundSummary[] = results.map((fund) =>
      this.mapFundToSummary(fund)
    );

    const pagination = createOffsetPagination<
      BankAccountRoboadvisorFundSummary
    >(
      data,
      pageSize,
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

  public async updateBankAccountRoboadvisorFund(
    fundId: number,
    payload: UpdateBankAccountRoboadvisorFundRequest,
  ): Promise<UpdateBankAccountRoboadvisorFundResponse> {
    const db = this.databaseService.get();

    // Verify fund exists
    const existing = await db
      .select()
      .from(roboadvisorFunds)
      .where(eq(roboadvisorFunds.id, fundId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existing) {
      throw new ServerError(
        "ROBOADVISOR_FUND_NOT_FOUND",
        `Roboadvisor fund with ID ${fundId} not found`,
        404,
      );
    }

    const updateValues: Record<string, unknown> = {
      updatedAt: new Date(),
    };

    if (payload.name !== undefined) updateValues.name = payload.name;
    if (payload.isin !== undefined) updateValues.isin = payload.isin;
    if (payload.assetClass !== undefined) {
      updateValues.assetClass = payload.assetClass;
    }
    if (payload.region !== undefined) updateValues.region = payload.region;
    if (payload.fundCurrencyCode !== undefined) {
      updateValues.fundCurrencyCode = payload.fundCurrencyCode;
    }
    if (payload.weight !== undefined) {
      updateValues.weight = payload.weight.toString();
    }
    if (payload.shareCount !== undefined) {
      updateValues.shareCount = payload.shareCount
        ? payload.shareCount.toString()
        : null;
    }

    const [result] = await db
      .update(roboadvisorFunds)
      .set(updateValues)
      .where(eq(roboadvisorFunds.id, fundId))
      .returning();

    this.calculateRoboadvisorValueAfterTax(existing.roboadvisorId).catch(
      (error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${existing.roboadvisorId}:`,
          error,
        );
      },
    );

    return this.mapFundToResponse(result);
  }

  public async deleteBankAccountRoboadvisorFund(fundId: number): Promise<void> {
    const db = this.databaseService.get();

    const existing = await db
      .select({ roboadvisorId: roboadvisorFunds.roboadvisorId })
      .from(roboadvisorFunds)
      .where(eq(roboadvisorFunds.id, fundId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existing) {
      throw new ServerError(
        "ROBOADVISOR_FUND_NOT_FOUND",
        `Roboadvisor fund with ID ${fundId} not found`,
        404,
      );
    }

    const result = await db
      .delete(roboadvisorFunds)
      .where(eq(roboadvisorFunds.id, fundId))
      .returning({ id: roboadvisorFunds.id });

    if (result.length === 0) {
      throw new ServerError(
        "ROBOADVISOR_FUND_NOT_FOUND",
        `Roboadvisor fund with ID ${fundId} not found`,
        404,
      );
    }

    this.calculateRoboadvisorValueAfterTax(existing.roboadvisorId).catch(
      (error) => {
        console.error(
          `Failed to trigger async calculation for roboadvisor ${existing.roboadvisorId}:`,
          error,
        );
      },
    );
  }

  // Private helper methods
  private getRoboadvisorSortColumn(sortField: BankAccountRoboadvisorSortField) {
    switch (sortField) {
      case BankAccountRoboadvisorSortField.Name:
        return roboadvisors.name;
      case BankAccountRoboadvisorSortField.CreatedAt:
        return roboadvisors.createdAt;
      case BankAccountRoboadvisorSortField.UpdatedAt:
        return roboadvisors.updatedAt;
      default:
        return roboadvisors.createdAt;
    }
  }

  private getBalanceSortColumn(
    sortField: BankAccountRoboadvisorBalanceSortField,
  ) {
    switch (sortField) {
      case BankAccountRoboadvisorBalanceSortField.Date:
        return roboadvisorBalances.date;
      case BankAccountRoboadvisorBalanceSortField.CreatedAt:
        return roboadvisorBalances.createdAt;
      default:
        return roboadvisorBalances.date;
    }
  }

  private mapRoboadvisorToResponse(
    roboadvisor: typeof roboadvisors.$inferSelect,
  ): CreateBankAccountRoboadvisorResponse {
    return {
      id: roboadvisor.id,
      name: roboadvisor.name,
      bankAccountId: roboadvisor.bankAccountId,
      riskLevel: roboadvisor.riskLevel,
      managementFeePercentage: parseFloat(roboadvisor.managementFeePercentage),
      custodyFeePercentage: parseFloat(roboadvisor.custodyFeePercentage),
      fundTerPercentage: parseFloat(roboadvisor.fundTerPercentage),
      totalFeePercentage: parseFloat(roboadvisor.totalFeePercentage),
      managementFeeFrequency: roboadvisor.managementFeeFrequency,
      custodyFeeFrequency: roboadvisor.custodyFeeFrequency,
      terPricedInNav: roboadvisor.terPricedInNav,
      taxPercentage: roboadvisor.taxPercentage
        ? parseFloat(roboadvisor.taxPercentage)
        : null,
      createdAt: toISOStringSafe(roboadvisor.createdAt),
      updatedAt: toISOStringSafe(roboadvisor.updatedAt),
    };
  }

  private mapRoboadvisorToSummary(
    roboadvisor: typeof roboadvisors.$inferSelect & {
      latestCalculation: {
        currentValue: string;
        currencyCode: string;
        calculatedAt: string;
      } | null;
    },
  ): BankAccountRoboadvisorSummary {
    return {
      id: roboadvisor.id,
      name: roboadvisor.name,
      bankAccountId: roboadvisor.bankAccountId,
      riskLevel: roboadvisor.riskLevel,
      managementFeePercentage: parseFloat(roboadvisor.managementFeePercentage),
      custodyFeePercentage: parseFloat(roboadvisor.custodyFeePercentage),
      fundTerPercentage: parseFloat(roboadvisor.fundTerPercentage),
      totalFeePercentage: parseFloat(roboadvisor.totalFeePercentage),
      managementFeeFrequency: roboadvisor.managementFeeFrequency,
      custodyFeeFrequency: roboadvisor.custodyFeeFrequency,
      terPricedInNav: roboadvisor.terPricedInNav,
      taxPercentage: roboadvisor.taxPercentage
        ? parseFloat(roboadvisor.taxPercentage)
        : null,
      createdAt: toISOStringSafe(roboadvisor.createdAt),
      updatedAt: toISOStringSafe(roboadvisor.updatedAt),
      latestCalculation: roboadvisor.latestCalculation
        ? {
          currentValue: roboadvisor.latestCalculation.currentValue.toString(),
          currencyCode: roboadvisor.latestCalculation.currencyCode,
          calculatedAt: toISOStringSafe(
            new Date(roboadvisor.latestCalculation.calculatedAt),
          ),
        }
        : null,
    };
  }

  private mapBalanceToResponse(
    balance: typeof roboadvisorBalances.$inferSelect,
  ): CreateBankAccountRoboadvisorBalanceResponse {
    return {
      id: balance.id,
      roboadvisorId: balance.roboadvisorId,
      date: balance.date,
      type: balance.type,
      amount: balance.amount,
      currencyCode: balance.currencyCode,
      createdAt: toISOStringSafe(balance.createdAt),
      updatedAt: toISOStringSafe(balance.updatedAt),
    };
  }

  private mapBalanceToSummary(
    balance: typeof roboadvisorBalances.$inferSelect,
  ): BankAccountRoboadvisorBalanceSummary {
    return {
      id: balance.id,
      roboadvisorId: balance.roboadvisorId,
      date: balance.date,
      type: balance.type,
      amount: balance.amount,
      currencyCode: balance.currencyCode,
      createdAt: toISOStringSafe(balance.createdAt),
      updatedAt: toISOStringSafe(balance.updatedAt),
    };
  }

  private mapFundToResponse(
    fund: typeof roboadvisorFunds.$inferSelect,
  ): CreateBankAccountRoboadvisorFundResponse {
    return {
      id: fund.id,
      roboadvisorId: fund.roboadvisorId,
      name: fund.name,
      isin: fund.isin,
      assetClass: fund.assetClass,
      region: fund.region,
      fundCurrencyCode: fund.fundCurrencyCode,
      weight: parseFloat(fund.weight),
      shareCount: fund.shareCount ? parseFloat(fund.shareCount) : null,
      createdAt: toISOStringSafe(fund.createdAt),
      updatedAt: toISOStringSafe(fund.updatedAt),
    };
  }

  private mapFundToSummary(
    fund: typeof roboadvisorFunds.$inferSelect,
  ): BankAccountRoboadvisorFundSummary {
    return {
      id: fund.id,
      roboadvisorId: fund.roboadvisorId,
      name: fund.name,
      isin: fund.isin,
      assetClass: fund.assetClass,
      region: fund.region,
      fundCurrencyCode: fund.fundCurrencyCode,
      weight: parseFloat(fund.weight),
      shareCount: fund.shareCount ? parseFloat(fund.shareCount) : null,
      createdAt: toISOStringSafe(fund.createdAt),
      updatedAt: toISOStringSafe(fund.updatedAt),
    };
  }

  private getFundSortColumn(sortField: BankAccountRoboadvisorFundSortField) {
    switch (sortField) {
      case BankAccountRoboadvisorFundSortField.Name:
        return roboadvisorFunds.name;
      case BankAccountRoboadvisorFundSortField.Isin:
        return roboadvisorFunds.isin;
      case BankAccountRoboadvisorFundSortField.AssetClass:
        return roboadvisorFunds.assetClass;
      case BankAccountRoboadvisorFundSortField.Region:
        return roboadvisorFunds.region;
      case BankAccountRoboadvisorFundSortField.FundCurrencyCode:
        return roboadvisorFunds.fundCurrencyCode;
      case BankAccountRoboadvisorFundSortField.Weight:
        return roboadvisorFunds.weight;
      case BankAccountRoboadvisorFundSortField.CreatedAt:
        return roboadvisorFunds.createdAt;
      case BankAccountRoboadvisorFundSortField.UpdatedAt:
        return roboadvisorFunds.updatedAt;
      default:
        return roboadvisorFunds.name;
    }
  }

  /**
   * Calculate roboadvisor portfolio value after capital gains tax
   * @param roboadvisorId - Roboadvisor ID
   * @returns Current portfolio value after tax, or null if unable to calculate
   */
  public async calculateRoboadvisorValueAfterTax(
    roboadvisorId: number,
  ): Promise<
    {
      currentValue: string;
      currencyCode: string;
    } | null
  > {
    try {
      const db = this.databaseService.get();

      // Get roboadvisor with capital gains tax percentage
      const roboadvisor = await db
        .select()
        .from(roboadvisors)
        .where(eq(roboadvisors.id, roboadvisorId))
        .limit(1)
        .then((rows) => rows[0]);

      if (!roboadvisor) {
        console.warn(`Roboadvisor with ID ${roboadvisorId} not found`);
        return null;
      }

      // Get all funds for this roboadvisor
      const funds = await db
        .select()
        .from(roboadvisorFunds)
        .where(eq(roboadvisorFunds.roboadvisorId, roboadvisorId));

      if (funds.length === 0) {
        console.warn(`No funds found for roboadvisor ${roboadvisorId}`);
        return null;
      }

      // Get all balances (deposits/withdrawals) for this roboadvisor
      const balances = await db
        .select()
        .from(roboadvisorBalances)
        .where(eq(roboadvisorBalances.roboadvisorId, roboadvisorId));

      if (balances.length === 0) {
        console.warn(`No balances found for roboadvisor ${roboadvisorId}`);
        return null;
      }

      // Calculate total invested amount (sum of deposits minus withdrawals)
      let totalInvested = 0;
      const currencyCode = balances[0].currencyCode;

      for (const balance of balances) {
        const amount = parseFloat(balance.amount);
        if (balance.type === "deposit" || balance.type === "adjustment") {
          totalInvested += amount;
        } else if (balance.type === "withdrawal") {
          totalInvested -= amount;
        }
      }

      // Get price provider
      const priceProvider = this.priceProviderFactory.getProvider();

      // Fetch current prices for all funds and calculate total portfolio value
      let totalCurrentValue = 0;
      let successfulPriceFetches = 0;
      let eligibleFundsCount = 0;

      for (const fund of funds) {
        // Skip funds without share count - cannot calculate value
        if (!fund.shareCount) {
          console.warn(
            `Fund ${fund.name} (ISIN: ${fund.isin}) has no share count, skipping`,
          );
          continue;
        }

        // Fund has valid shareCount, so it's eligible for price calculation
        eligibleFundsCount++;

        try {
          const priceFetchStartedAt = Date.now();
          console.info(
            `Starting index fund price fetch for roboadvisor ${roboadvisorId}, fund ${fund.name}, ISIN ${fund.isin}, currency ${currencyCode}`,
          );

          const priceString = await priceProvider.getCurrentPrice(
            fund.isin,
            currencyCode,
          );

          const priceFetchDurationMilliseconds =
            Date.now() - priceFetchStartedAt;

          if (!priceString) {
            console.warn(
              `Index fund price fetch returned no value for roboadvisor ${roboadvisorId}, fund ${fund.name}, ISIN ${fund.isin}, currency ${currencyCode} after ${priceFetchDurationMilliseconds}ms`,
            );
            continue;
          }

          console.info(
            `Index fund price fetch succeeded for roboadvisor ${roboadvisorId}, fund ${fund.name}, ISIN ${fund.isin}, currency ${currencyCode}, price ${priceString}, duration ${priceFetchDurationMilliseconds}ms`,
          );

          const currentPrice = parseFloat(priceString);
          const shareCount = parseFloat(fund.shareCount);

          // Validate parsed numbers before computing to avoid NaN propagation
          if (Number.isFinite(currentPrice) && Number.isFinite(shareCount)) {
            // Calculate current value: shares * currentPrice
            const fundCurrentValue = shareCount * currentPrice;
            totalCurrentValue += fundCurrentValue;
            successfulPriceFetches++;
          } else {
            console.warn(
              `Skipping fund ${fund.name} (ISIN: ${fund.isin}) due to invalid numeric values: price='${priceString}', shareCount='${fund.shareCount}'`,
            );
            // skip adding to totalCurrentValue
            continue;
          }
        } catch (error) {
          console.error(
            `Index fund price fetch failed for roboadvisor ${roboadvisorId}, fund ${fund.name}, ISIN ${fund.isin}, currency ${currencyCode}:`,
            error,
          );
          continue;
        }
      }

      // Return null if we couldn't fetch prices for most/all funds
      // This prevents returning misleading portfolio values
      if (
        successfulPriceFetches === 0 ||
        successfulPriceFetches < eligibleFundsCount / 2
      ) {
        console.warn(
          `Unable to calculate portfolio value: only ${successfulPriceFetches} out of ${eligibleFundsCount} eligible fund prices retrieved`,
        );
        return null;
      }

      // Calculate capital gain (can be negative for losses)
      const capitalGain = totalCurrentValue - totalInvested;

      // Apply tax only to gains (not to losses)
      const taxPercentage = roboadvisor.taxPercentage
        ? parseFloat(roboadvisor.taxPercentage)
        : 0;

      let valueAfterTax: number;

      if (capitalGain > 0) {
        // Tax only applies to the gain portion
        const taxAmount = capitalGain * taxPercentage;
        valueAfterTax = totalCurrentValue - taxAmount;
      } else {
        // No tax on losses
        valueAfterTax = totalCurrentValue;
      }

      // Store calculation
      await this.calculationsService.storeCalculation(
        roboadvisorId,
        valueAfterTax.toFixed(2),
      );

      return {
        currentValue: valueAfterTax.toFixed(2),
        currencyCode,
      };
    } catch (error) {
      console.error("Error calculating roboadvisor value after tax:", error);
      return null;
    }
  }

  /**
   * Calculate after-tax value for all roboadvisors
   */
  public async calculateAllRoboadvisors(): Promise<void> {
    const db = this.databaseService.get();

    // Get all roboadvisors
    const allRoboadvisors = await db
      .select({ id: roboadvisors.id })
      .from(roboadvisors);

    console.log(`Processing ${allRoboadvisors.length} roboadvisors`);

    // Calculate value after tax for each roboadvisor
    for (const roboadvisor of allRoboadvisors) {
      try {
        await this.calculateRoboadvisorValueAfterTax(roboadvisor.id);
      } catch (error) {
        console.error(
          `Failed to calculate roboadvisor ${roboadvisor.id}:`,
          error,
        );
      }
    }
  }
}
