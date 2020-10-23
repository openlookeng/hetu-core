import alt from "../queryeditor/alt";
import OverviewApiUtils from "./OverviewApiUtils";
class OverviewActions {
    constructor() {
        this.generateActions(
            'receiveData',
            'memoryData',
        )
    }
    getData(){
        OverviewApiUtils.getLineData().then((data)=>{
            this.actions.receiveData(data);
        });
    }
    getMemoryData(){
        OverviewApiUtils.getWorkMemory().then((data)=>{
            this.actions.memoryData(data);
        })
    }
}
export default alt.createActions(OverviewActions);