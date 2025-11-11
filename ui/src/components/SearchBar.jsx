
import React from 'react'
export default function SearchBar({ value, onChange, onSubmit, loading }) {
  return (
    <form className="flex gap-2" onSubmit={(e)=>{e.preventDefault(); onSubmit();}}>
      <input className="input" placeholder="Ex.: Quais são as disciplines dos Ventrue?" value={value} onChange={(e)=>onChange(e.target.value)} />
      <button className="btn" disabled={loading} type="submit">{loading ? 'Buscando...' : 'Buscar'}</button>
    </form>
  )
}
