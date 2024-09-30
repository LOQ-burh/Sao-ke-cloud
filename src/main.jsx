import React from "react";
import ReactDOM from "react-dom/client";
import { NextUIProvider } from "@nextui-org/react";
import App from "./App";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <NextUIProvider>
      <div style={{ padding: 10 }} className="w-screen  justify-center">
        <App />
      </div>  
    </NextUIProvider>
  </React.StrictMode>
);