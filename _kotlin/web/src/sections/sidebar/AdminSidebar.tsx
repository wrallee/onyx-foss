"use client";

import React, { useEffect, useRef, useState } from "react";
import { usePathname } from "next/navigation";
import { useTranslations } from "next-intl";
import { SidebarLayouts, useSidebarState } from "@opal/layouts";
import { InputTypeIn, SidebarTab } from "@opal/components";
import { SvgSearch, SvgX } from "@opal/icons";
import useFilter from "@/hooks/useFilter";
import {
  buildKotlinAdminItems,
  groupBySection,
  type SidebarItemEntry,
} from "@/lib/admin-sidebar-utils";
import { SvgOnyxLogo, SvgOnyxLogoTyped } from "@opal/logos";

function renderSidebarLogo(folded: boolean) {
  return folded ? SvgOnyxLogo : SvgOnyxLogoTyped;
}

const SECTION_LABELS: Record<string, string> = {
  craft: "Craft",
  agentsAndActions: "Agents & Actions",
  documentsAndKnowledge: "Documents & Knowledge",
  integrations: "Integrations",
  permissions: "Permissions",
  organization: "Organization",
  usage: "Usage",
};

export default function AdminSidebar() {
  const t = useTranslations("sidebar");
  const { folded, setFolded } = useSidebarState();
  const searchRef = useRef<HTMLInputElement>(null);
  const [focusSearch, setFocusSearch] = useState(false);
  const pathname = usePathname();
  const allItems = buildKotlinAdminItems();

  useEffect(() => {
    if (focusSearch && !folded && searchRef.current) {
      searchRef.current.focus();
      setFocusSearch(false);
    }
  }, [focusSearch, folded]);

  const { query, setQuery, filtered } = useFilter(
    allItems,
    (item: SidebarItemEntry) => item.label ?? item.nameId
  );
  const groups = groupBySection(filtered);

  return (
    <SidebarLayouts.Root>
      <SidebarLayouts.Header renderAppLogo={renderSidebarLogo} showLogoWhenFolded>
        {folded ? (
          <SidebarTab
            icon={SvgSearch}
            onClick={() => {
              setFolded(false);
              setFocusSearch(true);
            }}
          >
            {t("adminSidebar.search.label")}
          </SidebarTab>
        ) : (
          <InputTypeIn
            ref={searchRef}
            variant="internal"
            searchIcon
            placeholder={t("adminSidebar.searchInput.placeholder")}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            clearButton
          />
        )}
      </SidebarLayouts.Header>
      <SidebarLayouts.Body scrollKey="admin-sidebar">
        {groups.map((group, index) => (
          <React.Fragment key={index}>
            <SidebarLayouts.Section
              title={
                group.sectionId
                  ? SECTION_LABELS[group.sectionId]
                  : undefined
              }
            >
              {group.items.map(({ disabled, icon, label, link, nameId }) => (
                <SidebarTab
                  key={link}
                  icon={icon}
                  href={disabled ? undefined : link}
                  selected={!disabled && pathname.startsWith(link)}
                  disabled={disabled}
                  tooltip={
                    disabled ? "Not supported in this Kotlin port." : undefined
                  }
                >
                  {label ?? nameId}
                </SidebarTab>
              ))}
            </SidebarLayouts.Section>
          </React.Fragment>
        ))}
      </SidebarLayouts.Body>
      <SidebarLayouts.Footer>
        <SidebarTab icon={SvgX} href="/" variant="sidebar-light">
          {t("adminSidebar.exitAdminPanel.label")}
        </SidebarTab>
      </SidebarLayouts.Footer>
    </SidebarLayouts.Root>
  );
}
