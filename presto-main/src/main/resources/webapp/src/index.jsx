import React from "react";
import ReactDOM from "react-dom";
import {ClusterHUD} from "./components/ClusterHUD";
import {QueryList} from "./components/QueryList";
import {PageTitle} from "./components/PageTitle";

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);
