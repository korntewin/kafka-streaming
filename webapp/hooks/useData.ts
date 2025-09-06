import { useQuery, useInfiniteQuery } from "@tanstack/react-query";
import { ApiResponse } from "@/types";

interface UseDataParams {
  page?: number;
  limit?: number;
}

export const useData = ({ page = 1, limit = 10 }: UseDataParams = {}) => {
  return useQuery<ApiResponse>({
    queryKey: ["data", page, limit],
    queryFn: async () => {
      const response = await fetch(`/api/mongodb?page=${page}&limit=${limit}`);
      if (!response.ok) {
        throw new Error("Failed to fetch data");
      }
      return response.json();
    },
    refetchInterval: 5_000,
  });
};

interface UseInfiniteDataParams {
  limit?: number;
}

export const useInfiniteData = ({ limit = 25 }: UseInfiniteDataParams = {}) => {
  return useInfiniteQuery<ApiResponse>({
    queryKey: ["infiniteData", limit],
    queryFn: async ({ pageParam = 1 }) => {
      const response = await fetch(
        `/api/mongodb?page=${pageParam}&limit=${limit}`,
      );
      if (!response.ok) {
        throw new Error("Failed to fetch data");
      }
      return response.json();
    },
    getNextPageParam: (lastPage) => {
      const { page, pages } = lastPage.pagination;
      return page < pages ? page + 1 : undefined;
    },
    initialPageParam: 1,
    refetchInterval: 5_000,
  });
};
