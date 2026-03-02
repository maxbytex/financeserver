import { NetWorthCalculationService } from "../api/versions/v1/services/net-worth-calculation/net-worth-calculation-service.ts";
import { serviceContainer } from "../core/services/service-container.ts";

Deno.cron("calculate-net-worth", "0 0 * * *", async () => {
  const denoTimeline = Deno.env.get("DENO_TIMELINE");

  if (denoTimeline !== "production") {
    return;
  }

  const netWorthCalculationService = serviceContainer.get(
    NetWorthCalculationService,
  );

  await netWorthCalculationService.calculateAll();
});
