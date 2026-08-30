import { redirect } from "next/navigation";

export default function AppPage() {
  redirect("/admin/indexing/status");
}
