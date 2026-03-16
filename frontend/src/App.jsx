/**
 * Fund Doctor — Main App
 * Assembles all pages into the premium 360° MF platform
 */
import { useState, useEffect } from "react";
import { CSS, useAPI, Sidebar, OverviewPage } from "./shared.jsx";
import { ScorecardPage, CategoryPage, ExitPage, PortfolioPage } from "./pages1.jsx";
import { OverlapPage, AnalyticsPage } from "./pages2.jsx";

const API = "http://localhost:8000/api";

export default function App() {
  const [page, setPage] = useState("overview");
  const { data: pipe } = useAPI("/pipeline/status", []);

  useEffect(() => {
    const el = document.createElement("style");
    el.textContent = CSS;
    document.head.appendChild(el);
    return () => document.head.removeChild(el);
  }, []);

  const PAGE_MAP = {
    overview:  <OverviewPage setPage={setPage} />,
    scorecard: <ScorecardPage />,
    category:  <CategoryPage />,
    exit:      <ExitPage />,
    portfolio: <PortfolioPage />,
    overlap:   <OverlapPage />,
    analytics: <AnalyticsPage />,
  };

  return (
    <div style={{ display: "flex", minHeight: "100vh", background: "#060810" }}>
      <Sidebar page={page} setPage={setPage} pipe={pipe} />
      <main style={{ marginLeft: 200, flex: 1, padding: "30px 36px 80px", minHeight: "100vh", maxWidth: 1200 }}>
        {PAGE_MAP[page] || <OverviewPage setPage={setPage} />}
      </main>
    </div>
  );
}
