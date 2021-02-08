import React from "react";
import ReactDOM from "react-dom";
import Header from "./queryeditor/components/Header";
import Footer from "./queryeditor/components/Footer";

ReactDOM.render(
    <div className='flex flex-row flex-initial header'>
        <Header />
    </div>,
    document.getElementById('page-header')
);

ReactDOM.render(
    <div className='flex flex-row flex-initial footer'>
        <Footer/>
    </div>,
    document.getElementById('page-footer')
);
