"use client";

import { SidebarStateProvider, ToastProvider } from "@opal/layouts";

interface KotlinAdminProviderProps {
  children: React.ReactNode;
}

/** Admin-only providers without user/session or authorization state. */
export default function KotlinAdminProvider({
  children,
}: KotlinAdminProviderProps) {
  return (
      <SidebarStateProvider defaultFolded={false}>
        <ToastProvider>{children}</ToastProvider>
      </SidebarStateProvider>
  );
}
