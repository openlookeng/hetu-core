import xhr from "../queryeditor/utils/xhr";
export default {
    getLineData(){
        return xhr('../v1/cluster')
    },
    getWorkMemory(){
        return xhr('../v1/cluster/workerMemory')
    }
}
