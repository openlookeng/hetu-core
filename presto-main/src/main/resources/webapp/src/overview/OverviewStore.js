import alt from '../queryeditor/alt';
import OverviewActions from './OverviewActions';
class OverviewStore {
    constructor() {
        this.lineData=null;
        this.memoryData=null;
        this.requestNum=0;
        this.bindListeners({
            onReceiveData:OverviewActions.RECEIVE_DATA,
            onMemoryData:OverviewActions.MEMORY_DATA
        })
    }
    onReceiveData(data){
        this.lineData=data;
        this.requestNum++;
    }
    onMemoryData(data){
        this.memoryData = data;
        this.requestNum++;
    }
}
export default alt.createStore(OverviewStore,'OverviewStore')