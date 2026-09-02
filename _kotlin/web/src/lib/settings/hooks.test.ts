import { expect, test } from "@jest/globals";

import { useSettings } from "@/lib/settings/hooks";

test("provides the seven-day pruning default to connector creation", () => {
  expect(useSettings().default_pruning_freq).toBe(7 * 24 * 60 * 60);
});
