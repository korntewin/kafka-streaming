import { generateRandomName } from "@/lib/seededRandom";

export interface DataItem {
  _id: string;
  group_id: string;
  cumulative_score: number;
  event_count: number;
  avg_score: number;
  first_event_timestamp: number;
  last_event_timestamp: number;
  updated_at: number;
  lag_seconds?: number;
}

export interface PaginationInfo {
  page: number;
  limit: number;
  total: number;
  pages: number;
}

export interface ApiResponse {
  data: DataItem[];
  pagination: PaginationInfo;
}

export interface TableColumn {
  id: string;
  label: string;
  minWidth?: number;
  align?: "right" | "left" | "center";
  format?: (value: any) => string; // eslint-disable-line @typescript-eslint/no-explicit-any
}

export const columns: TableColumn[] = [
  {
    id: "_id",
    label: "Name",
    minWidth: 170,
    format: (value: string) => generateRandomName(value),
  },
  { id: "group_id", label: "Group ID", minWidth: 170 },
  {
    id: "cumulative_score",
    label: "Cumulative Score",
    minWidth: 170,
    format: (value: number) => value.toFixed(2),
  },
  { id: "event_count", label: "Event Count", minWidth: 170 },
  {
    id: "avg_score",
    label: "Average Score",
    minWidth: 170,
    format: (value: number) => value.toFixed(2),
  },
  {
    id: "lag_seconds",
    label: "Lag (seconds)",
    minWidth: 170,
    format: (value: number) => value.toFixed(2),
  },
  {
    id: "updated_at",
    label: "Updated At",
    minWidth: 170,
    format: (value: number) => {
      return new Date(value).toLocaleDateString("en-US", {
        day: "2-digit",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
      });
    },
  },
];
