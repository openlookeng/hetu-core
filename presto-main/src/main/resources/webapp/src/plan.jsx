import React from "react";
import ReactDOM from "react-dom";
import {LivePlan} from "./components/LivePlan";
import {getFirstParameter} from "./utils";

ReactDOM.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false}/>,
    document.getElementById('live-plan-container')
);
