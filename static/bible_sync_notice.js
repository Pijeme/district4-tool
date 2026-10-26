/* Shared notice for signed-in readers; detailed progress remains developer-only. */
(() => {
    "use strict";
    const notice=document.getElementById("bibleStudyUpdateNotice");
    if(!notice)return;
    const message="Bible Study is being updated. Some study resources may be temporarily unavailable. Please try again when the update finishes.";
    let timer=null,busy=false,authorized=true;
    function display(active){
        if(active && notice.textContent!==message)notice.textContent=message;
        notice.hidden=!active;
    }
    async function check(){
        clearTimeout(timer);
        if(busy || !authorized)return;
        busy=true;
        const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),10000);
        try{
            if(!document.hidden){
                const response=await fetch(notice.dataset.statusUrl,{credentials:"same-origin",cache:"no-store",signal:controller.signal});
                if(response.status===401 || response.status===403){authorized=false;display(false);return;}
                if(response.ok && (response.headers.get("Content-Type") || "").includes("application/json")){
                    const data=await response.json();if(data.ok)display(data.active===true);
                }
            }
        }catch(_error){/* Preserve the last known notice until the next successful check. */}
        finally{
            clearTimeout(timeout);busy=false;
            if(authorized)timer=setTimeout(check,notice.hidden?15000:5000);
        }
    }
    window.addEventListener("bible-study-sync-status",event=>{display(event.detail?.active===true);check();});
    window.addEventListener("focus",check);
    document.addEventListener("visibilitychange",()=>{if(!document.hidden)check();});
    check();
})();
