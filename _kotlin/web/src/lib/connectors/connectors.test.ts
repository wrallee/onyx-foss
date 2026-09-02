import { expect, test } from "@jest/globals";

import {
  connectorConfigs,
  createConnectorInitialValues,
  defaultRefreshFreqMinutes,
} from "@/lib/connectors/connectors";

test("uses a one-day refresh frequency for new connectors", () => {
  expect(defaultRefreshFreqMinutes).toBe(24 * 60);
});

test("starts Confluence cloud and attachment options unchecked", () => {
  const initialValues = createConnectorInitialValues("confluence");

  expect(initialValues.is_cloud).toBe(false);
  expect(initialValues.include_attachments).toBe(false);
});

test("starts Jira connector creation with the Project scope selected", () => {
  const indexingScope = connectorConfigs.jira.values.find(
    (field) => field.name === "indexing_scope"
  );

  expect(indexingScope?.type).toBe("tab");
  if (indexingScope?.type !== "tab") {
    throw new Error("Jira indexing scope must be a tab field");
  }
  expect(indexingScope.defaultTab).toBe("project");
});
