import AdminChrome from "@/layouts/chromes/AdminChrome";

export interface AdminSSChromeProps {
  children: React.ReactNode;
}

/** The Kotlin administration surface has no application-user sign-in flow. */
export default async function AdminSSChrome({ children }: AdminSSChromeProps) {
  return <AdminChrome>{children}</AdminChrome>;
}
