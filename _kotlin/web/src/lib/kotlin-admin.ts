export const KOTLIN_ADMIN_SUPPORTED_SOURCES = [
  "file",
  "jira",
  "confluence",
  "github",
] as const;

export type KotlinAdminSupportedSource =
  (typeof KOTLIN_ADMIN_SUPPORTED_SOURCES)[number];

export function isKotlinAdminSupportedSource(
  source: string
): source is KotlinAdminSupportedSource {
  return (KOTLIN_ADMIN_SUPPORTED_SOURCES as readonly string[]).includes(source);
}

export function isKotlinAdminConnectorRoute(pathname: string): boolean {
  const match = /^\/admin\/connectors\/([^/]+)\/?$/.exec(pathname);
  return (
    match !== null &&
    isKotlinAdminSupportedSource(match[1]!.replace(/-/g, "_"))
  );
}
