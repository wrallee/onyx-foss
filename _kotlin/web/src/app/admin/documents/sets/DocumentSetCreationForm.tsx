"use client";

import { Form, Formik } from "formik";
import { useTranslations } from "next-intl";
import { mutate } from "swr";
import * as Yup from "yup";
import { toast } from "@opal/layouts";
import { Button } from "@opal/components";
import { SWR_KEYS } from "@/lib/swr-keys";
import { ConnectorStatus, DocumentSetSummary } from "@/lib/types";
import { TextFormField } from "@/components/Field";
import { ConnectorMultiSelect } from "@/components/ConnectorMultiSelect";
import Text from "@/refresh-components/texts/Text";
import {
  createDocumentSet,
  updateDocumentSet,
  DocumentSetCreationRequest,
} from "./lib";

interface SetCreationPopupProps {
  ccPairs: ConnectorStatus<any, any>[];
  onClose: () => void;
  existingDocumentSet?: DocumentSetSummary;
}

export const DocumentSetCreationForm = ({
  ccPairs,
  onClose,
  existingDocumentSet,
}: SetCreationPopupProps) => {
  const t = useTranslations("admin.documents");
  const isUpdate = existingDocumentSet !== undefined;

  return (
    <div className="max-w-full mx-auto">
      <Formik<DocumentSetCreationRequest>
        initialValues={{
          name: existingDocumentSet?.name ?? "",
          description: existingDocumentSet?.description ?? "",
          cc_pair_ids:
            existingDocumentSet?.cc_pair_summaries.map(
              (ccPairSummary) => ccPairSummary.id
            ) ?? [],
          is_public: true,
          users: [],
          groups: [],
          federated_connectors: [],
        }}
        validationSchema={Yup.object()
          .shape({
            name: Yup.string().required(t("sets.form.name.required")),
            description: Yup.string().optional(),
            cc_pair_ids: Yup.array()
              .of(Yup.number().required())
              .min(1, t("sets.form.connectors.required")),
          })
          .required()}
        onSubmit={async (values, formikHelpers) => {
          formikHelpers.setSubmitting(true);
          const payload = {
            ...values,
            is_public: true,
            users: [],
            groups: [],
            federated_connectors: [],
          };
          const response = isUpdate
            ? await updateDocumentSet({ id: existingDocumentSet.id, ...payload })
            : await createDocumentSet(payload);
          formikHelpers.setSubmitting(false);

          if (!response.ok) {
            const errorMsg = await response.text();
            toast.error(
              isUpdate
                ? t("sets.form.updateFailed.toast", { detail: errorMsg })
                : t("sets.form.createFailed.toast", { detail: errorMsg })
            );
            return;
          }

          toast.success(
            isUpdate
              ? t("sets.form.updated.toast")
              : t("sets.form.created.toast")
          );
          await Promise.all([
            mutate(SWR_KEYS.documentSets),
            mutate(SWR_KEYS.documentSetsEditable),
          ]);
          onClose();
        }}
      >
        {(props) => (
          <Form className="space-y-6 w-full">
            <div className="space-y-4 w-full">
              <TextFormField
                name="name"
                label={t("sets.form.name.label")}
                placeholder={t("sets.form.name.placeholder")}
              />
              <TextFormField
                name="description"
                label={t("sets.form.description.label")}
                placeholder={t("sets.form.description.placeholder")}
                optional
              />
              <div className="space-y-2">
                <Text as="p" mainUiAction>
                  Public access
                </Text>
                <Text as="p" secondaryBody text03>
                  This document set is available to everyone in this deployment.
                </Text>
                <div className="flex flex-wrap gap-2">
                  <Button disabled prominence="secondary">
                    Public access enabled
                  </Button>
                  <Button disabled prominence="secondary">
                    User access disabled
                  </Button>
                  <Button disabled prominence="secondary">
                    Group access disabled
                  </Button>
                </div>
              </div>
            </div>

            <div className="border-t border-border-02" />

            <ConnectorMultiSelect
              name="cc_pair_ids"
              label={t("sets.form.connectors.label")}
              connectors={ccPairs}
              selectedIds={props.values.cc_pair_ids}
              onChange={(selectedIds) => {
                props.setFieldValue("cc_pair_ids", selectedIds);
              }}
              placeholder={t("sets.form.connectors.placeholder")}
            />

            <div className="flex pt-4 border-t border-border-02">
              <div className="mx-auto w-56">
                <Button type="submit" disabled={props.isSubmitting} width="full">
                  {isUpdate
                    ? t("sets.form.submitButton.updateLabel")
                    : t("sets.form.submitButton.createLabel")}
                </Button>
              </div>
            </div>
          </Form>
        )}
      </Formik>
    </div>
  );
};
