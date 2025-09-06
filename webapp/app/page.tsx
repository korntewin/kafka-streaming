"use client";

import React from "react";
import { Container, Typography, Box } from "@mui/material";
import DataTable from "@/components/DataTable";
import { columns } from "@/types";
import styles from "./page.module.css";

export default function Home() {
  return (
    <div className={styles.container}>
      <Container maxWidth="lg">
        <Box className={styles.header}>
          <Typography variant="h4" component="h1" className={styles.title}>
            Kafka Streaming Dashboard
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Display real-time aggregated data from Kafka to MongoDB
          </Typography>
        </Box>
        <DataTable columns={columns} />
      </Container>
    </div>
  );
}
