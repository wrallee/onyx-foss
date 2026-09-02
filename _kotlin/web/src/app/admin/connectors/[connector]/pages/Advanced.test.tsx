/// <reference types="jest" />

import { Formik } from "formik";

import AdvancedFormPage from "@/app/admin/connectors/[connector]/pages/Advanced";
import { render, screen } from "@tests/setup/test-utils";

test("describes the seven-day pruning default", () => {
  render(
    <Formik initialValues={{}} onSubmit={() => undefined}>
      <AdvancedFormPage />
    </Formik>
  );

  expect(screen.getByText(/Default is 168 hours \(7 days\)/)).not.toBeNull();
});
