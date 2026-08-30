import "./globals.css";

import type { Metadata } from "next";
import { MODAL_ROOT_ID } from "@/lib/constants";
import { DM_Mono, Hanken_Grotesk } from "next/font/google";
import { ThemeProvider } from "next-themes";
import { TooltipProvider } from "@radix-ui/react-tooltip";
import SWRConfigProvider from "@/providers/SWRConfigProvider";
import KotlinAdminProvider from "@/providers/KotlinAdminProvider";
import { NextIntlClientProvider } from "next-intl";
import { getLocale, getMessages } from "next-intl/server";

const hankenGrotesk = Hanken_Grotesk({
  subsets: ["latin"],
  display: "swap",
  fallback: ["-apple-system", "BlinkMacSystemFont", "Segoe UI", "Roboto"],
});

const dmMono = DM_Mono({
  weight: "400",
  subsets: ["latin"],
  display: "swap",
  fallback: [
    "SF Mono",
    "Monaco",
    "Cascadia Code",
    "Roboto Mono",
    "Consolas",
    "Courier New",
  ],
});

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "Onyx Admin" };

interface LayoutProps {
  children: React.ReactNode;
}

export default async function Layout({ children }: LayoutProps) {
  const locale = await getLocale();
  const messages = await getMessages();

  return (
    <html
      lang={locale}
      style={
        {
          "--font-hanken-grotesk": `${hankenGrotesk.style.fontFamily}, var(--font-cjk-sans), sans-serif`,
          "--font-dm-mono": `${dmMono.style.fontFamily}, var(--font-cjk-sans), monospace`,
        } as React.CSSProperties
      }
      suppressHydrationWarning
    >
      <head>
        <meta
          name="viewport"
          content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0, interactive-widget=resizes-content"
        />
      </head>
      <body className="relative font-hanken">
        <NextIntlClientProvider locale={locale} messages={messages}>
          <ThemeProvider
            attribute="class"
            defaultTheme="system"
            enableSystem
            disableTransitionOnChange
          >
            <div className="text-text min-h-screen bg-background">
              <TooltipProvider>
                <SWRConfigProvider>
                  <KotlinAdminProvider>
                      <div id={MODAL_ROOT_ID} className="h-screen w-screen">
                        {children}
                      </div>
                  </KotlinAdminProvider>
                </SWRConfigProvider>
              </TooltipProvider>
            </div>
          </ThemeProvider>
        </NextIntlClientProvider>
      </body>
    </html>
  );
}
