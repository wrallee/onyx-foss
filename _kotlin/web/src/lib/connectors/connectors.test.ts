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

test("configures GitHub connector with public github checkbox and dynamic base url", () => {
  // Check ordering: is_public_github is first (like Confluence), followed by github_base_url
  expect(connectorConfigs.github.values[0].name).toBe("is_public_github");
  expect(connectorConfigs.github.values[0].label).toBe("Use Github.com");
  expect(connectorConfigs.github.values[0].type).toBe("checkbox");
  expect(connectorConfigs.github.values[1].name).toBe("github_base_url");
  expect(connectorConfigs.github.values[1].label).toBe("GitHub API Base URL");
  expect(connectorConfigs.github.values[1].type).toBe("text");

  const isPublicField = connectorConfigs.github.values[0];
  expect((isPublicField as { default?: boolean })?.default).toBe(false);

  const initialValues = createConnectorInitialValues("github");
  expect(initialValues.is_public_github).toBe(false);
  expect(initialValues.github_base_url).toBe(
    (connectorConfigs.github.values[1] as any).default ?? undefined
  );

  const githubBaseUrlField = connectorConfigs.github.values[1];
  expect(githubBaseUrlField?.optional).toBe(false);

  // When public is checked: disabled=true, rightText=undefined, transform="https://api.github.com"
  const disabledFn = githubBaseUrlField?.disabled as (values: any) => boolean;
  const rightTextFn = githubBaseUrlField?.rightText as (
    values: any
  ) => string | undefined;
  const transformFn = githubBaseUrlField?.transform as (
    value: string,
    values: any
  ) => string;

  expect(disabledFn({ is_public_github: true })).toBe(true);
  expect(rightTextFn({ is_public_github: true })).toBeUndefined();
  expect(transformFn("anything", { is_public_github: true })).toBe(
    "https://api.github.com"
  );

  // When public is unchecked: disabled=false, rightText="/api/v3", transform appends /api/v3
  expect(disabledFn({ is_public_github: false })).toBe(false);
  expect(rightTextFn({ is_public_github: false })).toBe("/api/v3");
  expect(
    transformFn("https://github.my-company.com", { is_public_github: false })
  ).toBe("https://github.my-company.com/api/v3");
  expect(
    transformFn("https://github.my-company.com/", { is_public_github: false })
  ).toBe("https://github.my-company.com/api/v3");
  expect(
    transformFn("https://github.my-company.com/api/v3", {
      is_public_github: false,
    })
  ).toBe("https://github.my-company.com/api/v3");

  // isPublicField.onChange updates github_base_url value
  const mockSetFieldValue = jest.fn();
  (isPublicField as any)?.onChange(true, mockSetFieldValue);
  expect(mockSetFieldValue).toHaveBeenCalledWith(
    "github_base_url",
    "https://api.github.com"
  );

  (isPublicField as any)?.onChange(false, mockSetFieldValue);
  expect(mockSetFieldValue).toHaveBeenCalledWith("github_base_url", "");
});
