import { inject, injectable } from "@needle-di/core";
import { and, asc, desc, eq, getTableColumns, sql } from "drizzle-orm";
import { DatabaseService } from "../../../../../core/services/database-service.ts";
import {
  cryptoExchangeBalancesTable,
  cryptoExchangeCalculationsTable,
  cryptoExchangesTable,
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
import { CryptoExchangeBalanceSummary } from "../../interfaces/crypto-exchanges/crypto-exchange-balance-summary-interface.ts";
import type {
  CreateCryptoExchangeBalanceRequest,
  CreateCryptoExchangeBalanceResponse,
  GetCryptoExchangeBalancesResponse,
  UpdateCryptoExchangeBalanceRequest,
  UpdateCryptoExchangeBalanceResponse,
} from "../../schemas/crypto-exchange-balances-schemas.ts";
import { CryptoExchangeCalculationsService } from "../crypto-exchange-calculations/crypto-exchange-calculations-service.ts";
import { CryptoPriceProviderFactory } from "../external-pricing/factory/crypto-price-provider-factory.ts";

@injectable()
export class CryptoExchangeBalancesService {
  constructor(
    private databaseService = inject(DatabaseService),
    private calculationsService = inject(CryptoExchangeCalculationsService),
    private priceProviderFactory = inject(CryptoPriceProviderFactory),
  ) {}

  public async createCryptoExchangeBalance(
    exchangeId: number,
    payload: CreateCryptoExchangeBalanceRequest,
  ): Promise<CreateCryptoExchangeBalanceResponse> {
    const db = this.databaseService.get();

    const exchange = await db
      .select({ id: cryptoExchangesTable.id })
      .from(cryptoExchangesTable)
      .where(eq(cryptoExchangesTable.id, exchangeId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!exchange) {
      throw new ServerError(
        "CRYPTO_EXCHANGE_NOT_FOUND",
        `Crypto exchange with ID ${exchangeId} not found`,
        404,
      );
    }

    const [result] = await db
      .insert(cryptoExchangeBalancesTable)
      .values({
        cryptoExchangeId: exchangeId,
        balance: payload.balance,
        symbolCode: payload.symbolCode,
        investedAmount: payload.investedAmount ?? null,
        investedCurrencyCode: payload.investedCurrencyCode ?? null,
      })
      .returning();

    this.calculateCryptoValueAfterTax(exchangeId, payload.symbolCode).catch(
      (error) => {
        console.error(
          `Failed to trigger crypto value calculation for exchange ${exchangeId}:`,
          error,
        );
      },
    );

    return this.mapBalanceToResponse(result);
  }

  public async getCryptoExchangeBalances(payload: {
    cryptoExchangeId?: number;
    limit?: number;
    cursor?: string;
    sortOrder?: SortOrder;
  }): Promise<GetCryptoExchangeBalancesResponse> {
    const db = this.databaseService.get();
    const exchangeId = payload.cryptoExchangeId;
    const pageSize = payload.limit ?? DEFAULT_PAGE_SIZE;
    const cursor = payload.cursor;
    const sortOrder = payload.sortOrder ?? SortOrder.Desc;

    // Verify crypto exchange exists if exchangeId is provided
    if (exchangeId !== undefined) {
      const exchange = await db
        .select({ id: cryptoExchangesTable.id })
        .from(cryptoExchangesTable)
        .where(eq(cryptoExchangesTable.id, exchangeId))
        .limit(1)
        .then((rows) => rows[0]);

      if (!exchange) {
        throw new ServerError(
          "CRYPTO_EXCHANGE_NOT_FOUND",
          `Crypto exchange with ID ${exchangeId} not found`,
          404,
        );
      }
    }

    const size = Math.min(pageSize, MAX_PAGE_SIZE);
    const offset = cursor ? decodeCursor(cursor) : 0;

    const orderDirection = sortOrder === SortOrder.Asc ? asc : desc;

    const countQuery = db
      .select({ count: sql<number>`COUNT(*)` })
      .from(cryptoExchangeBalancesTable);

    if (exchangeId !== undefined) {
      countQuery.where(
        eq(cryptoExchangeBalancesTable.cryptoExchangeId, exchangeId),
      );
    }

    const [{ count }] = await countQuery;

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

    const query = db
      .select({
        ...getTableColumns(cryptoExchangeBalancesTable),
      })
      .from(cryptoExchangeBalancesTable);

    if (exchangeId !== undefined) {
      query.where(eq(cryptoExchangeBalancesTable.cryptoExchangeId, exchangeId));
    }

    const results = await query
      .orderBy(orderDirection(cryptoExchangeBalancesTable.createdAt))
      .limit(size)
      .offset(offset);

    const data: CryptoExchangeBalanceSummary[] = results.map((balance) =>
      this.mapBalanceToSummary(balance)
    );

    const pagination = createOffsetPagination<CryptoExchangeBalanceSummary>(
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

  public async updateCryptoExchangeBalance(
    balanceId: number,
    payload: UpdateCryptoExchangeBalanceRequest,
  ): Promise<UpdateCryptoExchangeBalanceResponse> {
    const db = this.databaseService.get();

    const existingBalance = await db
      .select({
        id: cryptoExchangeBalancesTable.id,
        cryptoExchangeId: cryptoExchangeBalancesTable.cryptoExchangeId,
        symbolCode: cryptoExchangeBalancesTable.symbolCode,
      })
      .from(cryptoExchangeBalancesTable)
      .where(eq(cryptoExchangeBalancesTable.id, balanceId))
      .limit(1)
      .then((rows) => rows[0]);

    if (!existingBalance) {
      throw new ServerError(
        "BALANCE_NOT_FOUND",
        `Balance with ID ${balanceId} not found`,
        404,
      );
    }

    const updateValues: {
      balance?: string;
      symbolCode?: string;
      investedAmount?: string | null;
      investedCurrencyCode?: string | null;
      updatedAt: Date;
    } = {
      updatedAt: new Date(),
    };

    if (payload.balance !== undefined) {
      updateValues.balance = payload.balance;
    }

    if (payload.symbolCode !== undefined) {
      updateValues.symbolCode = payload.symbolCode;
    }

    if (payload.investedAmount !== undefined) {
      updateValues.investedAmount = payload.investedAmount;
    }

    if (payload.investedCurrencyCode !== undefined) {
      updateValues.investedCurrencyCode = payload.investedCurrencyCode;
    }

    const [result] = await db
      .update(cryptoExchangeBalancesTable)
      .set(updateValues)
      .where(eq(cryptoExchangeBalancesTable.id, balanceId))
      .returning();

    // Trigger calculation for both old and new symbol if symbolCode changed
    this.calculateCryptoValueAfterTax(
      existingBalance.cryptoExchangeId,
      existingBalance.symbolCode,
    ).catch((error) => {
      console.error(
        `Failed to trigger crypto value calculation for exchange ${existingBalance.cryptoExchangeId} symbol ${existingBalance.symbolCode}:`,
        error,
      );
    });

    if (
      payload.symbolCode !== undefined &&
      payload.symbolCode !== existingBalance.symbolCode
    ) {
      this.calculateCryptoValueAfterTax(
        existingBalance.cryptoExchangeId,
        payload.symbolCode,
      ).catch((error) => {
        console.error(
          `Failed to trigger crypto value calculation for exchange ${existingBalance.cryptoExchangeId} symbol ${payload.symbolCode}:`,
          error,
        );
      });
    }

    return this.mapBalanceToResponse(result);
  }

  public async deleteCryptoExchangeBalance(balanceId: number): Promise<void> {
    const db = this.databaseService.get();

    // Verify balance exists before pushing telemetry
    const [existing] = await db
      .select({
        id: cryptoExchangeBalancesTable.id,
        cryptoExchangeId: cryptoExchangeBalancesTable.cryptoExchangeId,
        symbolCode: cryptoExchangeBalancesTable.symbolCode,
      })
      .from(cryptoExchangeBalancesTable)
      .where(eq(cryptoExchangeBalancesTable.id, balanceId))
      .limit(1);

    if (!existing) {
      throw new ServerError(
        "BALANCE_NOT_FOUND",
        `Balance with ID ${balanceId} not found`,
        404,
      );
    }

    await db
      .delete(cryptoExchangeBalancesTable)
      .where(eq(cryptoExchangeBalancesTable.id, balanceId));

    this.calculateCryptoValueAfterTax(
      existing.cryptoExchangeId,
      existing.symbolCode,
    ).catch((error) => {
      console.error(
        `Failed to trigger crypto value calculation for exchange ${existing.cryptoExchangeId} symbol ${existing.symbolCode}:`,
        error,
      );
    });
  }

  private mapBalanceToResponse(
    balance: typeof cryptoExchangeBalancesTable.$inferSelect,
  ): CreateCryptoExchangeBalanceResponse {
    return {
      id: balance.id,
      cryptoExchangeId: balance.cryptoExchangeId,
      balance: balance.balance,
      symbolCode: balance.symbolCode,
      investedAmount: balance.investedAmount,
      investedCurrencyCode: balance.investedCurrencyCode,
      createdAt: toISOStringSafe(balance.createdAt),
      updatedAt: toISOStringSafe(balance.updatedAt),
    };
  }

  private mapBalanceToSummary(
    balance: typeof cryptoExchangeBalancesTable.$inferSelect,
  ): CryptoExchangeBalanceSummary {
    return {
      id: balance.id,
      cryptoExchangeId: balance.cryptoExchangeId,
      balance: balance.balance,
      symbolCode: balance.symbolCode,
      investedAmount: balance.investedAmount,
      investedCurrencyCode: balance.investedCurrencyCode,
      createdAt: toISOStringSafe(balance.createdAt),
      updatedAt: toISOStringSafe(balance.updatedAt),
    };
  }

  /**
   * Calculate crypto balance value after capital gains tax
   * @param cryptoExchangeId - Crypto exchange ID
   * @param symbolCode - Crypto symbol (e.g., BTC, ETH)
   * @param exchange - Optional pre-fetched exchange object
   * @returns Current value after tax, or null if unable to calculate
   */
  public async calculateCryptoValueAfterTax(
    cryptoExchangeId: number,
    symbolCode: string,
    exchange?: typeof cryptoExchangesTable.$inferSelect,
  ): Promise<
    {
      currentValue: string;
      currencyCode: string;
    } | null
  > {
    try {
      const db = this.databaseService.get();

      // Get crypto exchange with capital gains tax percentage if not provided
      if (!exchange) {
        exchange = await db
          .select()
          .from(cryptoExchangesTable)
          .where(eq(cryptoExchangesTable.id, cryptoExchangeId))
          .limit(1)
          .then((rows) => rows[0]);
      }

      if (!exchange) {
        console.warn(`Crypto exchange with ID ${cryptoExchangeId} not found`);
        return null;
      }

      // Get balance for this symbol
      const balance = await db
        .select()
        .from(cryptoExchangeBalancesTable)
        .where(
          sql`${cryptoExchangeBalancesTable.cryptoExchangeId} = ${cryptoExchangeId} 
              AND ${cryptoExchangeBalancesTable.symbolCode} = ${symbolCode}`,
        )
        .limit(1)
        .then((rows) => rows[0]);

      if (!balance) {
        console.warn(
          `Balance not found for exchange ${cryptoExchangeId} and symbol ${symbolCode}. Clearing calculations.`,
        );

        // Clear stale calculations for this symbol if balance is missing
        await db
          .delete(cryptoExchangeCalculationsTable)
          .where(
            and(
              eq(
                cryptoExchangeCalculationsTable.cryptoExchangeId,
                cryptoExchangeId,
              ),
              eq(cryptoExchangeCalculationsTable.symbolCode, symbolCode),
            ),
          );

        return null;
      }

      // Get invested amount and currency
      if (!balance.investedAmount || !balance.investedCurrencyCode) {
        console.warn(
          `Invested amount or currency not set for balance ${balance.id}`,
        );
        return null;
      }

      const investedAmount = parseFloat(balance.investedAmount);
      const currencyCode = balance.investedCurrencyCode;

      // Get price provider
      const priceProvider = this.priceProviderFactory.getProvider();

      // Fetch current price for the crypto symbol
      const priceFetchStartedAt = Date.now();
      console.info(
        `Starting crypto price fetch for exchange ${cryptoExchangeId}, symbol ${symbolCode}, currency ${currencyCode}`,
      );

      let priceString: string | null;

      try {
        priceString = await priceProvider.getCurrentPrice(
          symbolCode,
          currencyCode,
        );
      } catch (error) {
        console.error(
          `Crypto price fetch failed for exchange ${cryptoExchangeId}, symbol ${symbolCode}, currency ${currencyCode}:`,
          error,
        );
        return null;
      }

      const priceFetchDurationMilliseconds = Date.now() - priceFetchStartedAt;

      if (!priceString) {
        console.warn(
          `Crypto price fetch returned no value for exchange ${cryptoExchangeId}, symbol ${symbolCode}, currency ${currencyCode} after ${priceFetchDurationMilliseconds}ms`,
        );
        return null;
      }

      console.info(
        `Crypto price fetch succeeded for exchange ${cryptoExchangeId}, symbol ${symbolCode}, currency ${currencyCode}, price ${priceString}, duration ${priceFetchDurationMilliseconds}ms`,
      );

      // Calculate current value
      const cryptoBalance = parseFloat(balance.balance);
      const currentPrice = parseFloat(priceString);
      const currentValue = cryptoBalance * currentPrice;

      // Calculate capital gain (can be negative for losses)
      const capitalGain = currentValue - investedAmount;

      // Apply tax only to gains (not to losses)
      const taxPercentage = exchange.taxPercentage
        ? parseFloat(exchange.taxPercentage)
        : 0;

      let valueAfterTax: number;

      if (capitalGain > 0) {
        // Tax only applies to the gain portion
        const taxAmount = capitalGain * taxPercentage;
        valueAfterTax = currentValue - taxAmount;
      } else {
        // No tax on losses
        valueAfterTax = currentValue;
      }

      // Store calculation
      await this.calculationsService.storeCalculation(
        cryptoExchangeId,
        symbolCode,
        valueAfterTax.toFixed(2),
      );

      return {
        currentValue: valueAfterTax.toFixed(2),
        currencyCode,
      };
    } catch (error) {
      console.error("Error calculating crypto value after tax:", error);
      return null;
    }
  }

  /**
   * Calculate after-tax value for all crypto exchange balances
   */
  public async calculateAllCryptoBalances(): Promise<void> {
    const db = this.databaseService.get();

    // Get all crypto exchange balances
    const balances = await db
      .select({
        cryptoExchangeId: cryptoExchangeBalancesTable.cryptoExchangeId,
        symbolCode: cryptoExchangeBalancesTable.symbolCode,
      })
      .from(cryptoExchangeBalancesTable);

    console.log(`Processing ${balances.length} crypto balances`);

    // Calculate value after tax for each balance
    for (const balance of balances) {
      try {
        await this.calculateCryptoValueAfterTax(
          balance.cryptoExchangeId,
          balance.symbolCode,
        );
      } catch (error) {
        console.error(
          `Failed to calculate crypto balance for exchange ${balance.cryptoExchangeId} symbol ${balance.symbolCode}:`,
          error,
        );
      }
    }
  }
}
