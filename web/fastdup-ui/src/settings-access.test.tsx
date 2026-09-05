import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { WebUsersSettings, CertificateSettings } from './settings-access';
import { I18nProvider } from './i18n';
afterEach(cleanup);
it('creates a web account, requires matching passwords, and clears the secret', async()=>{
 const request=vi.fn().mockResolvedValueOnce([{username:'admin',mustChangePassword:false}]).mockResolvedValueOnce({username:'alice',mustChangePassword:true});
 render(<I18nProvider><WebUsersSettings request={request} username="admin" changePassword={()=>{}}/></I18nProvider>);
 await screen.findByText('admin');
 fireEvent.change(screen.getByLabelText(/^Benutzername/),{target:{value:'alice'}});
 fireEvent.change(screen.getByLabelText('Startpasswort'),{target:{value:'initial-alice-password'}});
 fireEvent.change(screen.getByLabelText('Startpasswort wiederholen'),{target:{value:'different-password'}});
 fireEvent.click(screen.getByRole('button',{name:'Benutzer anlegen'}));
 expect(await screen.findByRole('alert')).toHaveTextContent('Die Passwörter stimmen nicht überein.');expect(request).toHaveBeenCalledTimes(1);
 fireEvent.change(screen.getByLabelText('Startpasswort wiederholen'),{target:{value:'initial-alice-password'}});
 fireEvent.click(screen.getByRole('button',{name:'Benutzer anlegen'}));
 expect(await screen.findByRole('status')).toHaveTextContent('Benutzer alice wurde angelegt.');
 expect(screen.getByLabelText('Startpasswort')).toHaveValue('');expect(screen.getByText('Passwortwechsel beim nächsten Login erforderlich')).toBeVisible();
});
it('keeps the displayed certificate on failed PFX import',async()=>{
 const request=vi.fn().mockRejectedValue(new Error('Invalid PFX password'));const updated=vi.fn();
 render(<I18nProvider><CertificateSettings request={request} fingerprint="AA:BB" regenerate={()=>{}} onImported={updated}/></I18nProvider>);
 fireEvent.change(screen.getByLabelText(/^PFX-Datei/),{target:{files:[new File(['fixture'],'test.pfx')]}});
 fireEvent.change(screen.getByLabelText(/^PFX-Passwort/),{target:{value:'incorrect'}});
 fireEvent.submit(screen.getByRole('button',{name:'Importieren & aktivieren'}).closest('form')!);
 await waitFor(()=>expect(screen.getByRole('alert')).toHaveTextContent('Invalid PFX password'));
 expect(updated).not.toHaveBeenCalled();expect(screen.getByText('AA:BB')).toBeVisible();
});
