import React, { useState, useEffect, useRef, useCallback } from "react";
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  CircularProgress,
  Alert,
  Box,
  Typography,
  Switch,
  FormControlLabel,
} from "@mui/material";
import { useData, useInfiniteData } from "@/hooks/useData";
import { DataItem, TableColumn } from "@/types";
import styles from "./DataTable.module.css";

interface DataTableProps {
  columns: TableColumn[];
}

export default function DataTable({ columns }: DataTableProps) {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [isLazyMode, setIsLazyMode] = useState(true);
  const observerRef = useRef<HTMLDivElement>(null);

  const { data, isLoading, error, isError } = useData({
    page: page + 1,
    limit: rowsPerPage,
  });

  const {
    data: infiniteData,
    isLoading: isInfiniteLoading,
    error: infiniteError,
    isError: isInfiniteError,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteData({ limit: 25 });

  const [lagSeconds, setLagSeconds] = useState(0);

  const flattenedData = infiniteData?.pages.flatMap((page) => page.data) || [];
  const totalCount = infiniteData?.pages[0]?.pagination.total || 0;

  const handleObserver = useCallback(
    (entries: IntersectionObserverEntry[]) => {
      const [target] = entries;
      if (target.isIntersecting && hasNextPage && !isFetchingNextPage) {
        fetchNextPage();
      }
    },
    [fetchNextPage, hasNextPage, isFetchingNextPage],
  );

  useEffect(() => {
    if (!isLazyMode || !observerRef.current) return;

    const element = observerRef.current;
    const option = { threshold: 0.1 };
    const observer = new IntersectionObserver(handleObserver, option);
    observer.observe(element);

    return () => observer.unobserve(element);
  }, [handleObserver, isLazyMode]);

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPage(+event.target.value);
    setPage(0);
  };

  const handleModeToggle = (event: React.ChangeEvent<HTMLInputElement>) => {
    setIsLazyMode(event.target.checked);
  };

  const currentLoading = isLazyMode ? isInfiniteLoading : isLoading;
  const currentError = isLazyMode ? infiniteError : error;
  const currentIsError = isLazyMode ? isInfiniteError : isError;
  const rawData = isLazyMode ? flattenedData : data?.data || [];
  const [currentData, setCurrentData] = useState<DataItem[]>([]);
  const currentTotal = isLazyMode ? totalCount : data?.pagination?.total || 0;

  useEffect(() => {
    setCurrentData((prev) => {
      for (let i = 0; i < rawData.length; i++) {
        const newData = rawData[i];
        const prevData = prev[i];
        if (prevData && prevData.updated_at !== newData.updated_at) {
          setLagSeconds(Math.abs(newData.updated_at - prevData.updated_at) / 1000);
          break;
        }
      }
      return rawData;
    });
  }, [rawData]);

  if (currentLoading && (!isLazyMode || flattenedData.length === 0)) {
    return (
      <Box className={styles.loadingContainer}>
        <CircularProgress />
        <Typography variant="body1" sx={{ mt: 2 }}>
          Loading data...
        </Typography>
      </Box>
    );
  }

  if (currentIsError) {
    return (
      <Alert severity="error" className={styles.errorAlert}>
        {currentError instanceof Error
          ? currentError.message
          : "An error occurred while fetching data"}
      </Alert>
    );
  }

  return (
    <Paper className={styles.tableContainer}>
      {/* Mode Toggle */}
      <Box sx={{ p: 2, borderBottom: 1, borderColor: "divider", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <Box>
          <FormControlLabel
            control={
              <Switch
                checked={isLazyMode}
                onChange={handleModeToggle}
                color="primary"
              />
            }
            label={`${isLazyMode ? "Lazy Loading" : "Pagination"} Mode`}
          />
          <Typography variant="caption" sx={{ ml: 2 }}>
            {isLazyMode
              ? `Loaded ${currentData.length} of ${currentTotal} items`
              : `Page ${page + 1} of ${Math.ceil(currentTotal / rowsPerPage)}`}
          </Typography>
        </Box>
        <Typography variant="caption">
          Data Lag: {lagSeconds} seconds
        </Typography>
      </Box>

      <TableContainer sx={{ maxHeight: "70vh" }}>
        <Table stickyHeader aria-label="data table">
          <TableHead>
            <TableRow>
              {columns.map((column) => (
                <TableCell
                  key={column.id}
                  align={column.align}
                  style={{ minWidth: column.minWidth }}
                  className={styles.headerCell}
                >
                  {column.label}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {currentData?.map((row: DataItem, index) => (
              <TableRow
                hover
                role="checkbox"
                tabIndex={-1}
                key={row._id || index}
              >
                {columns.map((column) => {
                  // @ts-expect-error tested
                  const value = row[column.id];
                  return (
                    <TableCell key={column.id} align={column.align}>
                      {column.format ? column.format(value) : value}
                    </TableCell>
                  );
                })}
              </TableRow>
            ))}

            {isLazyMode && (
              <>
                {isFetchingNextPage && (
                  <TableRow>
                    <TableCell colSpan={columns.length}>
                      <Box display="flex" justifyContent="center" p={2}>
                        <CircularProgress size={24} />
                        <Typography variant="body2" sx={{ ml: 1 }}>
                          Loading more...
                        </Typography>
                      </Box>
                    </TableCell>
                  </TableRow>
                )}

                <TableRow>
                  <TableCell
                    colSpan={columns.length}
                    sx={{ p: 0, border: "none" }}
                  >
                    <div ref={observerRef} style={{ height: "20px" }} />
                  </TableCell>
                </TableRow>

                {!hasNextPage && currentData.length > 0 && (
                  <TableRow>
                    <TableCell colSpan={columns.length}>
                      <Box display="flex" justifyContent="center" p={2}>
                        <Typography variant="body2" color="textSecondary">
                          All items loaded ({currentData.length} total)
                        </Typography>
                      </Box>
                    </TableCell>
                  </TableRow>
                )}
              </>
            )}
          </TableBody>
        </Table>
      </TableContainer>

      {!isLazyMode && (
        <TablePagination
          rowsPerPageOptions={[10, 25, 100]}
          component="div"
          count={currentTotal}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
          className={styles.pagination}
        />
      )}
    </Paper>
  );
}
