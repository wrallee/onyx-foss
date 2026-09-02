/// <reference types="jest" />

import { Suspense, type ReactNode } from "react";
import useSWR from "swr";

import Page from "@/app/admin/connector/[ccPairId]/page";
import {
  CCPairFullInfo,
  ConnectorCredentialPairStatus,
} from "@/app/admin/connector/[ccPairId]/types";
import usePaginatedFetch from "@/hooks/usePaginatedFetch";
import { ValidSources } from "@/lib/types";
import { render, screen } from "@tests/setup/test-utils";

jest.mock("next/navigation", () => ({
  useRouter: () => ({ push: jest.fn() }),
}));

jest.mock("swr", () => ({
  __esModule: true,
  default: jest.fn(),
  mutate: jest.fn(),
  SWRConfig: ({ children }: { children: ReactNode }) => children,
}));

jest.mock("@/hooks/usePaginatedFetch", () => ({
  __esModule: true,
  default: jest.fn(),
}));

jest.mock("@/app/admin/connector/[ccPairId]/ReIndexModal", () => ({
  useReIndexModal: () => ({
    showReIndexModal: jest.fn(),
    ReIndexModal: null,
  }),
}));

jest.mock("@/app/admin/connector/[ccPairId]/useStatusChange", () => ({
  useStatusChange: () => ({
    handleStatusChange: jest.fn(),
    isUpdating: false,
    ConfirmModal: null,
  }),
}));

const ccPair: CCPairFullInfo = {
  id: 1,
  name: "Web connector",
  status: ConnectorCredentialPairStatus.ACTIVE,
  in_repeated_error_state: false,
  num_docs_indexed: 0,
  connector: {
    id: 1,
    name: "Web connector",
    source: ValidSources.Web,
    input_type: "load_state",
    connector_specific_config: {},
    refresh_freq: 24 * 60 * 60,
    prune_freq: 7 * 24 * 60 * 60,
    indexing_start: null,
    access_type: "public",
    credential_ids: [1],
    time_created: "2026-09-02T00:00:00Z",
    time_updated: "2026-09-02T00:00:00Z",
  },
  credential: {
    id: 1,
    credential_json: {},
    admin_public: true,
    source: ValidSources.Web,
    user_id: null,
    user_email: null,
    time_created: "2026-09-02T00:00:00Z",
    time_updated: "2026-09-02T00:00:00Z",
  },
  number_of_index_attempts: 0,
  last_index_attempt_status: null,
  latest_deletion_attempt: null,
  access_type: "public",
  is_editable_for_current_user: false,
  permissions: {},
  deletion_failure_message: null,
  indexing: false,
  creator: null,
  creator_email: null,
  last_indexed: null,
  last_pruned: null,
  last_full_permission_sync: null,
  overall_indexing_speed: null,
  latest_checkpoint_description: null,
  last_permission_sync_attempt_status: null,
  permission_syncing: false,
  last_permission_sync_attempt_finished: null,
  last_permission_sync_attempt_error_message: null,
  supports_targeted_reindex: false,
};

test("shows connector Advanced content without a collapsible toggle", async () => {
  jest.mocked(useSWR).mockReturnValue({
    data: ccPair,
    isLoading: false,
    error: undefined,
  } as ReturnType<typeof useSWR>);
  jest.mocked(usePaginatedFetch).mockReturnValue({
    currentPageData: null,
    isLoading: false,
    currentPage: 1,
    totalPages: 0,
    goToPage: jest.fn(),
    totalItems: 0,
    error: null,
    refresh: async () => undefined,
  });

  const params = Object.assign(Promise.resolve({ ccPairId: "1" }), {
    status: "fulfilled",
    value: { ccPairId: "1" },
  });

  render(
    <Suspense fallback={null}>
      <Page params={params} />
    </Suspense>
  );

  expect(await screen.findByText("Advanced Configuration")).not.toBeNull();
  expect(screen.queryByRole("button", { name: "Advanced" })).toBeNull();
});
