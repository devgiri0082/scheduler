import { z } from "zod";

export const RecordType = z.object({
  _id: z.number(),
  'מס רשיון': z.number().nullable(),
  'שם המתווך': z.string().nullable(),
  'עיר מגורים': z.string().nullable(),
});

export const FieldType = z.object({
  id: z.string(),
  type: z.string(),
});

export const LinksType = z.object({
  start: z.string(),
  next: z.string(),
});

export const AxiosResponseType = z.object({
  filters: z.record(z.unknown()),
  include_total: z.boolean(),
  limit: z.number(),
  offset: z.number(),
  q: z.string(),
  records_format: z.string(),
  resource_id: z.string(),
  total_estimation_threshold: z.null(),
  records: z.array(RecordType),
  fields: z.array(FieldType),
  _links: LinksType,
  total: z.number(),
  total_was_estimated: z.boolean(),
});

export type fetchDataType = {
  offset: number;
  limit: number;
  api: string
}
export type Record = z.infer<typeof RecordType>;
export type Records = Record[];

export type DataSet = {
  currentMonthData: Records,
  currentMonth: number,
  currentYear: number,
  nextMonthData: Records,
  nextMonth: number,
  nextYear: number
}