from __future__ import annotations
import csv, io, json, tempfile, zipfile
from pathlib import Path
from typing import Any, Dict, List
from .topology import TopologyContext

SEVERITY_MAP={1:'INFO',2:'WARNING',3:'WARNING',4:'ERROR',5:'CRITICAL'}

class InputPackageLoader:
    """Load the three related hackathon datasets from one ZIP without mixing context rows with alarms."""
    def load(self, zip_path: str) -> Dict[str, Any]:
        with zipfile.ZipFile(zip_path) as z:
            files=[n for n in z.namelist() if not n.endswith('/') and not n.startswith('__MACOSX/')]
            alarms=[]; inventory=[]; dependencies=[]; selected={}
            for name in files:
                suffix=Path(name).suffix.lower()
                if suffix not in {'.json','.csv'}: continue
                raw=z.read(name).decode('utf-8-sig',errors='replace')
                if suffix=='.json':
                    try: data=json.loads(raw)
                    except Exception: continue
                    if isinstance(data,list) and data and isinstance(data[0],dict) and {'alarm_id','timestamp','host','service','severity','alarm_type'}.issubset(data[0]):
                        if not alarms: alarms=data; selected['alarms']=name
                else:
                    rows=list(csv.DictReader(io.StringIO(raw)))
                    if not rows: continue
                    cols=set(rows[0])
                    if {'host','servis','veri_merkezi','kabin','ortam','is_kritikligi'}.issubset(cols):
                        inventory=rows; selected['host_inventory']=name
                    elif {'kaynak_servis','hedef_servis','bagimlilik_tipi','kritiklik'}.issubset(cols):
                        dependencies=rows; selected['service_dependencies']=name
                    elif {'alarm_id','timestamp','host','service','severity','alarm_type'}.issubset(cols) and not alarms:
                        alarms=rows; selected['alarms']=name
            if not alarms: raise ValueError('ZIP içinde alarm_id/timestamp/host/service/severity/alarm_type kolonlarını içeren alarm verisi bulunamadı.')
            if not inventory: raise ValueError('ZIP içinde host inventory kolonları bulunamadı.')
            if not dependencies: raise ValueError('ZIP içinde service dependency kolonları bulunamadı.')

        topology=TopologyContext(inventory,dependencies)
        normalized=[]; quality={'inventory_missing':0,'service_mismatch':0,'dc_mismatch':0,'rack_mismatch':0}
        for src in alarms:
            r=dict(src); tags=dict(r.get('tags') or {})
            try: source_sev=int(r.get('severity'))
            except Exception: source_sev=None
            inv=topology.host_context(r.get('host'))
            if not inv: quality['inventory_missing']+=1
            if inv and str(inv.get('servis')) != str(r.get('service')): quality['service_mismatch']+=1
            if inv and tags.get('veri_merkezi') and str(inv.get('veri_merkezi')) != str(tags.get('veri_merkezi')): quality['dc_mismatch']+=1
            if inv and tags.get('kabin') and str(inv.get('kabin')) != str(tags.get('kabin')): quality['rack_mismatch']+=1
            # Keep all source columns. JsonParser moves non-canonical fields to attributes.
            r['source_severity']=source_sev
            r['severity']=SEVERITY_MAP.get(source_sev, r.get('severity'))
            r['inventory_service']=inv.get('servis')
            r['business_criticality']=inv.get('is_kritikligi')
            r['data_center']=tags.get('veri_merkezi') or inv.get('veri_merkezi')
            r['rack']=tags.get('kabin') or inv.get('kabin')
            r['environment']=tags.get('ortam') or inv.get('ortam')
            normalized.append(r)

        tmp=tempfile.NamedTemporaryFile(delete=False,suffix='.jsonl',mode='w',encoding='utf-8',newline='')
        try:
            for r in normalized: tmp.write(json.dumps(r,ensure_ascii=False)+'\n')
        finally: tmp.close()
        return {
            'alarm_file':tmp.name, 'normalized_alarms': normalized, 'topology':topology,
            'summary':{'files':selected,'alarm_count':len(normalized),'host_count':len(inventory),'dependency_count':len(dependencies),'data_quality':quality,'inventory_columns':list(inventory[0].keys()) if inventory else [],'dependency_columns':list(dependencies[0].keys()) if dependencies else [],'alarm_columns':list(alarms[0].keys()) if alarms else []},
        }
