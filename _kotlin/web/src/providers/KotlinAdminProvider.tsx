"use client";

import { SidebarStateProvider, ToastProvider } from "@opal/layouts";
import { UserProvider } from "@/providers/UserProvider";

interface KotlinAdminProviderProps {
  children: React.ReactNode;
}

/** Providers required by the authentication-free Kotlin administration UI. */
export default function KotlinAdminProvider({
  children,
}: KotlinAdminProviderProps) {
  return (
    <UserProvider>
      <SidebarStateProvider defaultFolded={false}>
        <ToastProvider>{children}</ToastProvider>
      </SidebarStateProvider>
    </UserProvider>
  );
}
