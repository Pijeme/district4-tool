/* Shared notice for signed-in readers; detailed progress remains developer-only. */
(() => {
    "use strict";
    const notice=document.getElementById("bibleStudyUpdateNotice");
    if(!notice)return;
    const message="Bible Study is being updated. Some study resources may be temporarily unavailable. Please try again when the update finishes.";
    let timer=null,busy=false,authorized=true,developerPanel=null;
    function display(active){
        if(active && !notice.firstChild){
            const summary=document.createElement("div");summary.textContent=message;notice.append(summary);
        }
        notice.hidden=!active;
        if(!active && developerPanel){developerPanel.remove();developerPanel=null;}
    }
    function developerProgress(report,detailsUrl){
        if(!developerPanel){developerPanel=document.createElement("div");developerPanel.className="site-study-developer-progress";notice.append(developerPanel);}
        developerPanel.replaceChildren();
        const state=report.progress || {},title=document.createElement("strong"),bar=document.createElement("progress"),measure=document.createElement("div"),link=document.createElement("a");
        title.textContent=report.message || "Bible Study sync in progress";
        bar.max=100;bar.setAttribute("aria-label","Developer Bible Study sync progress");
        const done=Number(state.completed),total=Number(state.total);
        if(report.status==="running"&&state.completed!=null&&state.total!=null&&total>0&&Number.isFinite(done)&&Number.isFinite(total)){
            bar.value=Math.min(100,Math.max(0,done/total*100));
            measure.textContent=done.toLocaleString()+" / "+total.toLocaleString()+" "+(state.unit || "items")+" · "+bar.value.toFixed(1)+"% of this stage";
        }else{
            bar.removeAttribute("value");measure.textContent="Sync is running; waiting for measured stage progress.";
        }
        developerPanel.append(title,bar,measure);
        if(state.source){const source=document.createElement("div");source.textContent=state.source;developerPanel.append(source);}
        if(state.sources_total!=null){const count=document.createElement("div");count.textContent=(state.source_group || "Study sources prepared")+": "+Number(state.sources_done || 0).toLocaleString()+" / "+Number(state.sources_total).toLocaleString();developerPanel.append(count);}
        link.href=detailsUrl;link.textContent="View sync details";
        link.addEventListener("click",event=>{
            const button=document.getElementById("bsSyncDetails");
            if(button){event.preventDefault();button.click();}
        });
        developerPanel.append(link);
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
                    const data=await response.json();
                    if(data.ok){
                        display(data.active===true);
                        if(data.active===true && data.progress_url && data.details_url){
                            if(!developerPanel)developerProgress({status:"running",message:"Reading sync progress…",progress:{}},data.details_url);
                            const details=await fetch(data.progress_url,{credentials:"same-origin",cache:"no-store",signal:controller.signal});
                            if(details.ok && (details.headers.get("Content-Type") || "").includes("application/json")){
                                const snapshot=await details.json();if(snapshot.ok)developerProgress(snapshot.sync,data.details_url);
                            }else if(details.status===401 || details.status===403){
                                if(developerPanel){developerPanel.remove();developerPanel=null;}
                            }
                        }else if(developerPanel){developerPanel.remove();developerPanel=null;}
                    }
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
