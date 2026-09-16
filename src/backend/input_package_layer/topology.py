from __future__ import annotations
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional

class TopologyContext:
    """Read-only service/host topology supplied by the hackathon package."""
    def __init__(self, inventory: Iterable[Dict[str, Any]] = (), dependencies: Iterable[Dict[str, Any]] = ()):
        self.inventory = {str(r.get('host','')).strip(): dict(r) for r in inventory if str(r.get('host','')).strip()}
        self.dependencies = [dict(r) for r in dependencies]
        self.depends_on = defaultdict(set)       # source -> targets it depends on
        self.dependents = defaultdict(set)       # target -> sources affected by target
        self.edge_meta = {}
        for r in self.dependencies:
            src=str(r.get('kaynak_servis','')).strip(); dst=str(r.get('hedef_servis','')).strip()
            if not src or not dst: continue
            self.depends_on[src].add(dst); self.dependents[dst].add(src); self.edge_meta[(src,dst)] = r

    def host_context(self, host: str) -> Dict[str, Any]:
        return dict(self.inventory.get(str(host or '').strip(), {}))

    def dependency_path(self, dependent: str, root: str, max_hops: int = 4) -> Optional[List[str]]:
        """Return dependent -> ... -> root path, following declared depends-on direction."""
        dependent=str(dependent or ''); root=str(root or '')
        if not dependent or not root: return None
        if dependent == root: return [dependent]
        q=deque([(dependent,[dependent])]); seen={dependent}
        while q:
            node,path=q.popleft()
            if len(path)-1 >= max_hops: continue
            for nxt in self.depends_on.get(node, ()):
                if nxt == root: return path+[nxt]
                if nxt not in seen:
                    seen.add(nxt); q.append((nxt,path+[nxt]))
        return None

    def relation(self, a: str, b: str, max_hops: int = 4) -> Dict[str, Any] | None:
        """Describe causal service relation. Root is the service depended upon."""
        p = self.dependency_path(a,b,max_hops)
        if p: return {'dependent':a,'root':b,'path':p,'hops':len(p)-1}
        p = self.dependency_path(b,a,max_hops)
        if p: return {'dependent':b,'root':a,'path':p,'hops':len(p)-1}
        return None
