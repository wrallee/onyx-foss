import type { Metadata } from "next";
import AdminSSChrome from "@/layouts/chromes/AdminSSChrome";
import {
  generateAdminTitleMetadata,
  generateFaviconMetadata,
} from "@/lib/app/svcSS";

export async function generateMetadata(): Promise<Metadata> {
  return {
    title: await generateAdminTitleMetadata(),
    icons: await generateFaviconMetadata(),
  };
}

export interface AdminLayoutProps {
  children: React.ReactNode;
}

export default async function AdminLayout({ children }: AdminLayoutProps) {
  return await AdminSSChrome({ children });
}
