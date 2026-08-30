import { SourceIcon } from "@/components/SourceIcon";
import Link from "next/link";
import type { Route } from "next";
import { SourceMetadata } from "@/lib/search/interfaces";
import Text from "@/refresh-components/texts/Text";
import { isKotlinAdminSupportedSource } from "@/lib/kotlin-admin";

interface SourceTileProps {
  sourceMetadata: SourceMetadata;
  preSelect?: boolean;
  navigationUrl: string;
  hasExistingSlackCredentials: boolean;
}

export default function SourceTile({
  sourceMetadata,
  preSelect,
  navigationUrl,
}: SourceTileProps) {
  const supported = isKotlinAdminSupportedSource(sourceMetadata.internalName);
  const className = `flex flex-col items-center justify-center p-4 rounded-lg w-40 shadow-md bg-background-tint-00 relative ${
    supported
      ? "cursor-pointer hover:bg-background-tint-02"
      : "opacity-50 cursor-not-allowed"
  } ${preSelect && supported ? "subtle-pulse" : ""}`;
  const contents = (
    <>
      <SourceIcon sourceType={sourceMetadata.internalName} iconSize={24} />
      <Text as="p" className="pt-2">
        {sourceMetadata.displayName}
      </Text>
      {!supported && (
        <Text as="p" secondaryBody text03 className="pt-1">
          Not supported
        </Text>
      )}
    </>
  );

  if (!supported) {
    return (
      <div
        className={className}
        aria-disabled="true"
        title="Not supported in this Kotlin port."
      >
        {contents}
      </div>
    );
  }

  return (
    <Link className={className} href={navigationUrl as Route}>
      {contents}
    </Link>
  );
}
