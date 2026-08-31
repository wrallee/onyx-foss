import { render, screen } from "@tests/setup/test-utils";
import KotlinAdminProvider from "@/providers/KotlinAdminProvider";
import { useUser } from "@/providers/UserProvider";

function UserContextProbe() {
  const { userResolution } = useUser();
  return <span>{userResolution}</span>;
}

describe("KotlinAdminProvider", () => {
  it("provides user context to shared administration components", () => {
    render(
      <KotlinAdminProvider>
        <UserContextProbe />
      </KotlinAdminProvider>
    );

    expect(screen.getByText("resolved")).toBeInTheDocument();
  });
});
