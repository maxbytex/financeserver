import { inject, injectable } from "@needle-di/core";
import { BankAccountInterestRatesService } from "../bank-account-interest-rates/bank-account-interest-rates-service.ts";
import { BankAccountRoboadvisorsService } from "../bank-account-roboadvisors/bank-account-roboadvisors-service.ts";
import { CryptoExchangeBalancesService } from "../crypto-exchanges-balances/crypto-exchange-balances-service.ts";

@injectable()
export class NetWorthCalculationService {
  constructor(
    private interestRatesService = inject(BankAccountInterestRatesService),
    private roboadvisorsService = inject(BankAccountRoboadvisorsService),
    private cryptoBalancesService = inject(CryptoExchangeBalancesService),
  ) {}

  /**
   * Calculate all investments (batch operation)
   * Iterates through all bank accounts with interest rates, roboadvisors, and crypto balances
   * and performs after-tax calculations for each
   */
  public async calculateAll(): Promise<void> {
    const results = await Promise.allSettled([
      this.interestRatesService.calculateAllBankAccountInterestRates(),
      this.roboadvisorsService.calculateAllRoboadvisors(),
      this.cryptoBalancesService.calculateAllCryptoBalances(),
    ]);

    const failures = results.filter(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );

    if (failures.length > 0) {
      for (const failure of failures) {
        console.error("A net worth calculation task failed:", failure.reason);
      }

      throw new Error(
        `Net worth calculation partially failed. ${failures.length} of ${results.length} tasks failed.`,
      );
    }

    console.log("Net worth calculation completed successfully");
  }
}
