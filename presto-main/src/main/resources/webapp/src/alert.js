window.alert = function(msg){
    const body = document.getElementsByTagName('body')[0];

    var alertMask = document.createElement('div');
    var alertContainer = document.createElement('div');
    var alertDes = document.createElement('div');
    var alertConfirmParent = document.createElement('div');
    var alertConfirmBtn = document.createElement('button');

    css(alertMask, {
        "position" : "fixed",	/*生成绝对定位的元素，相对于浏览器窗口进行定位*/
        "display": "flex",
        "flex-direction": "row",
        "align-items": "center",
        "justify-content": "center",
        "width": "100%",
        "height": "100%",
        "top": "0",
        "left": "0",
        "z-index": "99999",
        "background": "#0000002b",
    })

    css(alertContainer, {
        "min-width": "500px",	/*容器最小240px*/
        "max-width": "580px",	/*容器最大320px*/
        "background":"#fff",
        "border": "1px solid #0000002b",
        "border-radius": "3px",
        "color": "#1f0000",
        "overflow": "hidden",
    })

    css(alertDes, {
        "padding": "35px 30px",
        "text-align": "center",
        "letter-spacing": "1px",
        "font-size": "14px",
        "color": "#1f0000",
    })

    css(alertConfirmParent, {
        "width": "100%",
        "padding": "20px 30px",
        "text-align": "right",
        "box-sizing": "border-box",
        "background": "#f2f2f2",
    })

    css(alertConfirmBtn, {
        "cursor": "pointer",
        "padding": "8px 10px",
        "border": "none",
        "border-radius": "2px",
        "color": "#fff",
        "background-color": "gray",
        "box-shadow": "0 0 2px gray",
    })

    body.append(alertMask);
    alertMask.append(alertContainer);
    alertContainer.append(alertDes);
    alertContainer.append(alertConfirmParent);
    alertConfirmParent.append(alertConfirmBtn);
    alertConfirmBtn.innerText = '确定';

    //加载提示信息
    alertDes.innerHTML = msg;
    //关闭当前alert弹窗
    function alertBtnClick(){
        body.removeChild(alertMask);
    }
    alertConfirmBtn.addEventListener("click", alertBtnClick);
}

function css(targetObj, cssObj) {
    var str = targetObj.getAttribute("style") ? targetObj.getAttribute("style") : "";
    for(var i in cssObj) {
        str += i + ":" + cssObj[i] + ";";
    }
    targetObj.style.cssText = str;
}