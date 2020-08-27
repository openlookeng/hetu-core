import React from 'react';

import { Tab, TabList, TabPanel, Tabs } from 'react-tabs';
import SchemaTree from "./SchemaTree";
import ColumnsPreview from "./ColumnsPreview";
import TabStore from "../stores/TabStore";
import TabActions from "../actions/TabActions";


class LeftPanel extends React.Component {
    constructor(args) {
        super(args);
        this.state = {
            selectedTab: TabStore.getSelectedLeftPanelTab()
        };
        this.onTabSelect = this.onTabSelect.bind(this);
        this.onTabChange = this.onTabChange.bind(this);
    }

    componentDidMount() {
        TabStore.listen(this.onTabChange);
        document.getElementById("leftpanel").addEventListener("contextmenu", (e) => {
            e.preventDefault();
            return false;
        });
    }

    componentWillUnmount() {
        TabStore.unlisten(this.onTabChange);
    }

    onTabSelect(selectedTab) {
        TabActions.selectLeftPanelTab(selectedTab);
    }

    onTabChange() {
        const selectedTab = TabStore.getSelectedLeftPanelTab();
        this.setState({selectedTab});
    }

    render() {
        return (
            <div id="leftpanel">
                <Tabs forceRenderTabPanel={true} className="leftTabContainer" onSelect={this.onTabSelect} selectedIndex={this.state.selectedTab}>
                    <TabList>
                        <Tab>Schema</Tab>
                        <Tab>Columns</Tab>
                    </TabList>
                    <TabPanel>
                        <SchemaTree/>
                    </TabPanel>
                    <TabPanel>
                        <ColumnsPreview/>
                    </TabPanel>
                </Tabs>
            </div>
        )
    }
}
export default LeftPanel;