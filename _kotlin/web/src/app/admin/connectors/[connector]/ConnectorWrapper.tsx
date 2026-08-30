"use client";

import { useTranslations } from "next-intl";
import { ConfigurableSources } from "@/lib/types";
import AddConnector from "./AddConnectorPage";
import { FormProvider } from "@/components/context/FormContext";
import CreateConnectorSidebar from "@/sections/sidebar/CreateConnectorSidebar";
import { AdminCustomSidebarPortal } from "@/layouts/chromes/AdminChrome";
import { HeaderTitle } from "@/components/header/HeaderTitle";
import { Button } from "@opal/components";
import { isValidSource } from "@/lib/sources";
import { isKotlinAdminSupportedSource } from "@/lib/kotlin-admin";

interface ConnectorWrapperProps {
  connector: ConfigurableSources;
}

function UnsupportedConnector({ connector }: ConnectorWrapperProps) {
  const t = useTranslations("admin.connectorsList");

  return (
    <div className="mt-12 w-full max-w-3xl mx-auto">
      <div className="mx-auto flex flex-col gap-y-2">
        <HeaderTitle>
          <p>{t("invalidConnector.title", { connector })}</p>
        </HeaderTitle>
        <p className="text-text-03">
          This connector is not supported in this Kotlin port.
        </p>
        <div className="mr-auto">
          <Button href="/admin/add-connector">Choose a supported connector</Button>
        </div>
      </div>
    </div>
  );
}

export default function ConnectorWrapper({ connector }: ConnectorWrapperProps) {
  if (!isValidSource(connector) || !isKotlinAdminSupportedSource(connector)) {
    return <UnsupportedConnector connector={connector} />;
  }

  return (
    <FormProvider connector={connector}>
      <AdminCustomSidebarPortal>
        <CreateConnectorSidebar />
      </AdminCustomSidebarPortal>
      <div className="mt-12 w-full max-w-3xl mx-auto">
        <AddConnector connector={connector} />
      </div>
    </FormProvider>
  );
}
