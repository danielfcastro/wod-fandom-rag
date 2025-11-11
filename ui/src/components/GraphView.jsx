
import React, { useEffect, useRef } from 'react'
import { Network } from 'vis-network/standalone'
export default function GraphView({ data }) {
  const ref = useRef(null)
  useEffect(()=>{
    if(!ref.current) return
    const nodes=[]; const edges=[]; let i=1; const root='root'
    nodes.push({id:root,label:'Resultado (Grafo)',shape:'box'})
    for(const obj of data||[]){ const key=Object.keys(obj)[0]; const val=obj[key]; const id='n'+(i++); nodes.push({id,label:`${key}: ${val}`}); edges.push({from:root,to:id}) }
    const net=new Network(ref.current,{nodes,edges},{physics:{stabilization:true},height:'300px',interaction:{hover:true}})
    return ()=>net?.destroy?.()
  },[data])
  return <div className="card"><div ref={ref} style={{height:300}} /></div>
}
