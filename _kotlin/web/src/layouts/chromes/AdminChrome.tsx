"use client";

import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { usePathname } from "next/navigation";
import AdminSidebar from "@/sections/sidebar/AdminSidebar";
import useScreenSize from "@/hooks/useScreenSize";
import { Button } from "@opal/components";
import { SvgSidebar } from "@opal/icons";
import { RootLayout, useSidebarState } from "@opal/layouts";
import { isKotlinAdminConnectorRoute } from "@/lib/kotlin-admin";

export interface AdminChromeProps {
  children: React.ReactNode;
}

const AdminCustomSidebarSlotContext = createContext<HTMLElement | null>(null);

export function AdminCustomSidebarPortal({
  children,
}: {
  children: ReactNode;
}) {
  const slot = useContext(AdminCustomSidebarSlotContext);
  if (!slot) return null;
  return createPortal(children, slot);
}

export default function AdminChrome({ children }: AdminChromeProps) {
  const { setFolded } = useSidebarState();
  const { isMobile } = useScreenSize();
  const pathname = usePathname();
  const [customSidebarSlot, setCustomSidebarSlot] =
    useState<HTMLDivElement | null>(null);
  const hasCustomSidebar = isKotlinAdminConnectorRoute(pathname);

  useEffect(() => {
    document.title = "Admin — Onyx";
  }, [pathname]);

  return (
    <AdminCustomSidebarSlotContext.Provider value={customSidebarSlot}>
      <RootLayout.Root>
        {hasCustomSidebar ? (
          <div ref={setCustomSidebarSlot} className="contents" />
        ) : (
          <AdminSidebar />
        )}
        <RootLayout.App data-main-container>
          {isMobile && (
            <RootLayout.Header>
              <div className="h-full flex items-center px-4 py-2">
                <Button
                  prominence="internal"
                  icon={SvgSidebar}
                  aria-label="Open Sidebar"
                  onClick={() => setFolded(false)}
                />
              </div>
            </RootLayout.Header>
          )}
          <RootLayout.MainContent>{children}</RootLayout.MainContent>
        </RootLayout.App>
      </RootLayout.Root>
    </AdminCustomSidebarSlotContext.Provider>
  );
}
