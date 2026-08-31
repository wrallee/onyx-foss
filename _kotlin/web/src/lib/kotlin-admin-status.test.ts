import { getCCPairDisplayStatus } from "@/app/admin/connector/[ccPairId]/lib";
import { ConnectorCredentialPairStatus } from "@/app/admin/connector/[ccPairId]/types";

describe("getCCPairDisplayStatus", () => {
  it("shows initial indexing before the first attempt", () => {
    expect(
      getCCPairDisplayStatus(ConnectorCredentialPairStatus.ACTIVE, null)
    ).toBe(ConnectorCredentialPairStatus.INITIAL_INDEXING);
  });

  it("keeps active after a successful attempt", () => {
    expect(
      getCCPairDisplayStatus(ConnectorCredentialPairStatus.ACTIVE, "success")
    ).toBe(ConnectorCredentialPairStatus.ACTIVE);
  });
});
