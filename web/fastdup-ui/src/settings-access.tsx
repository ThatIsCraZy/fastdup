import { useEffect, useState, type FormEvent } from "react";
import { KeyRound, ShieldCheck, UserPlus, RefreshCcw, Upload } from "lucide-react";
import { useI18n } from "./i18n";
import { Button } from "./components/ui/button";
import { Card, CardContent, CardHeader } from "./components/ui/card";

export type SettingsRequest = <T>(path: string, init?: RequestInit) => Promise<T>;
interface WebUser { username: string; mustChangePassword: boolean }
export function WebUsersSettings({ request, username, changePassword }: { request: SettingsRequest; username: string; changePassword: () => void }) {
  const { t } = useI18n();
  const [users, setUsers] = useState<WebUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [repeat, setRepeat] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  useEffect(() => { let active = true; request<WebUser[]>('/api/v1/users').then(value=>{if(active)setUsers(value)}).catch(error=>{if(active)setError(error.message)}).finally(()=>{if(active)setLoading(false)});return()=>{active=false} }, [request]);
  const create = async (event: FormEvent) => {
    event.preventDefault();setError('');setSuccess('');
    if(password!==repeat){setError(t('Die Passwörter stimmen nicht überein.'));return}
    setBusy(true);
    try {
      const user = await request<WebUser>('/api/v1/users',{method:'POST',body:JSON.stringify({username:name,password})});
      setUsers(current=>[...current,user].sort((a,b)=>a.username.localeCompare(b.username)));
      setSuccess(t('Benutzer {name} wurde angelegt.',{name:user.username}));setName('');setPassword('');setRepeat('');
    } catch(error){setError((error as Error).message)} finally{setBusy(false)}
  };
  return <div className="settings-cards">
    <Card><CardHeader><div><h2>{t('Web-Benutzer')}</h2><p>{t('Diese Konten haben Administratorrechte für die Web-Oberfläche. SMB-Zugänge werden separat verwaltet.')}</p></div></CardHeader><CardContent>
      {loading?<p>{t('Lädt')}</p>:<div className="settings-user-list">{users.map(user=><div key={user.username}><div><strong>{user.username}</strong><small>{user.mustChangePassword?t('Passwortwechsel beim nächsten Login erforderlich'):t('Administrator')}</small></div>{user.username===username && <Button variant="secondary" onClick={changePassword}><KeyRound size={15}/>{t('Passwort ändern')}</Button>}</div>)}</div>}
    </CardContent></Card>
    <Card><CardHeader><div><h2>{t('Neuer Web-Benutzer')}</h2><p>{t('Das Startpasswort muss beim ersten Login geändert werden.')}</p></div><UserPlus size={19}/></CardHeader><CardContent>
      <form className="settings-form" onSubmit={event=>void create(event)}>
        <label className="field"><span>{t('Benutzername')}</span><input autoComplete="off" required maxLength={32} pattern="[a-zA-Z0-9._\-]+" value={name} onChange={e=>setName(e.target.value)}/><small>{t('1–32 Zeichen: Buchstaben, Zahlen, Punkt, Bindestrich oder Unterstrich.')}</small></label>
        <label className="field"><span>{t('Startpasswort')}</span><input type="password" autoComplete="new-password" required minLength={12} maxLength={128} value={password} onChange={e=>setPassword(e.target.value)}/></label>
        <label className="field"><span>{t('Startpasswort wiederholen')}</span><input type="password" autoComplete="new-password" required minLength={12} maxLength={128} value={repeat} onChange={e=>setRepeat(e.target.value)}/></label>
        {error&&<p className="form-error" role="alert">{error}</p>}{success&&<p className="settings-success" role="status">{success}</p>}
        <Button disabled={busy||loading} type="submit"><UserPlus size={15}/>{t(busy?'Wird angelegt…':'Benutzer anlegen')}</Button>
      </form>
    </CardContent></Card>
  </div>;
}

export function CertificateSettings({request,fingerprint,regenerate,onImported}:{request:SettingsRequest;fingerprint:string;regenerate:()=>void;onImported:(fingerprint:string)=>void}) {
  const {t}=useI18n();const[file,setFile]=useState<File|null>(null);const[password,setPassword]=useState('');const[busy,setBusy]=useState(false);const[error,setError]=useState('');const[success,setSuccess]=useState(false);
  const upload=async(event:FormEvent)=>{
    event.preventDefault();setError('');setSuccess(false);if(!file)return;
    if(file.size>1048576){setError(t('Die PFX-Datei darf höchstens 1 MiB groß sein.'));return}
    setBusy(true);
    try{
      const pfx=await new Promise<string>((resolve,reject)=>{const reader=new FileReader();reader.onload=()=>resolve(String(reader.result).split(',')[1]);reader.onerror=()=>reject(new Error(t('Datei konnte nicht gelesen werden.')));reader.readAsDataURL(file)});
      const result=await request<{certificateFingerprint:string}>('/api/v1/tls/import',{method:'POST',body:JSON.stringify({pfx,password})});
      onImported(result.certificateFingerprint);setPassword('');setSuccess(true);
    }catch(error){setError((error as Error).message)}finally{setBusy(false)}
  };
  return <div className="settings-cards">
    <Card><CardHeader><div><h2>{t('Aktives HTTPS-Zertifikat')}</h2><p>{t('Die Identität der Web-Oberfläche wird ohne Appliance-Reboot aktualisiert.')}</p></div><ShieldCheck size={19}/></CardHeader><CardContent className="fingerprint"><small>SHA-256</small><code>{fingerprint}</code><Button variant="secondary" disabled={busy} onClick={regenerate}><RefreshCcw size={15}/>{t('Selbstsigniertes Zertifikat erzeugen')}</Button></CardContent></Card>
    <Card><CardHeader><div><h2>{t('PFX-Zertifikat importieren')}</h2><p>{t('PKCS#12-Datei (.pfx oder .p12) mit Serverzertifikat, Private Key und optionaler Zertifikatskette.')}</p></div><Upload size={19}/></CardHeader><CardContent>
      <form className="settings-form" onSubmit={event=>void upload(event)}>
        <label className="field"><span>{t('PFX-Datei')}</span><input type="file" accept=".pfx,.p12,application/x-pkcs12" required onChange={e=>{setFile(e.target.files?.[0]??null);setSuccess(false)}}/><small>{t('Maximal 1 MiB. Ein ungültiger Import verändert das aktive Zertifikat nicht.')}</small></label>
        <label className="field"><span>{t('PFX-Passwort')}</span><input type="password" autoComplete="off" maxLength={1024} value={password} onChange={e=>setPassword(e.target.value)}/><small>{t('Bei einer unverschlüsselten PFX-Datei leer lassen.')}</small></label>
        {error&&<p className="form-error" role="alert">{error}</p>}{success&&<p className="settings-success" role="status">{t('Zertifikat importiert und aktiviert.')}</p>}
        <Button type="submit" disabled={busy||!file}><Upload size={15}/>{t(busy?'Wird geprüft…':'Importieren & aktivieren')}</Button>
      </form>
    </CardContent></Card>
  </div>;
}

export function SambaUsersSettings({ request, onCreated }: { request: SettingsRequest; onCreated: () => void }) {
  const { t } = useI18n();
  const [users, setUsers] = useState<string[]>([]);
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [repeat, setRepeat] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  useEffect(() => { let active = true; request<string[]>('/api/v1/samba/users').then(value => { if(active) setUsers(value); }).catch(error => { if(active) setError(error.message); }); return () => { active = false; }; }, [request]);
  const create = async (event: FormEvent) => {
    event.preventDefault(); setError(''); setSuccess('');
    if(password !== repeat) { setError(t('Die Passwörter stimmen nicht überein.')); return; }
    setBusy(true);
    try {
      const result = await request<{username:string}>('/api/v1/samba/users', { method:'POST', body:JSON.stringify({username:name,password}) });
      setUsers(current => [...current,result.username].sort());
      setName(''); setPassword(''); setRepeat('');
      setSuccess(t('SMB-Benutzer {name} wurde angelegt. Bitte in der Freigabe erlauben.', {name:result.username}));
      onCreated();
    } catch(error) { setError((error as Error).message); } finally { setBusy(false); }
  };
  return <div className="settings-cards">
    <Card><CardHeader><div><h2>{t('SMB-Benutzer')}</h2><p>{t('Diese Konten gelten für SMB-Freigaben. Sie haben keinen WebUI- oder SSH-Zugang.')}</p></div></CardHeader><CardContent>
      <div className="settings-user-list">{users.map(user => <div key={user}><strong>{user}</strong></div>)}</div>
      {!users.length && <p>{t('Noch keine SMB-Benutzer vorhanden.')}</p>}
    </CardContent></Card>
    <Card><CardHeader><h2>{t('SMB-Benutzer anlegen')}</h2></CardHeader><CardContent>
      <form className="settings-form" onSubmit={create}>
        <label className="field"><span>{t('SMB-Benutzername')}</span><input required maxLength={32} pattern="[a-z][a-z0-9_-]*" autoComplete="off" value={name} onChange={event=>setName(event.target.value)}/></label>
        <label className="field"><span>{t('SMB-Passwort')}</span><input type="password" required minLength={12} maxLength={256} autoComplete="new-password" value={password} onChange={event=>setPassword(event.target.value)}/></label>
        <label className="field"><span>{t('SMB-Passwort wiederholen')}</span><input type="password" required autoComplete="new-password" value={repeat} onChange={event=>setRepeat(event.target.value)}/></label>
        {error && <p className="form-error" role="alert">{error}</p>}
        {success && <p className="settings-success" role="status">{success}</p>}
        <Button type="submit" disabled={busy}><UserPlus size={15}/>{t(busy?'Wird angelegt…':'SMB-Benutzer anlegen')}</Button>
      </form>
    </CardContent></Card>
  </div>;
}
