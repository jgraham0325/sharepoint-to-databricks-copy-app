# SharePoint Upload App — One-Pager

---

## The Problem

**Current (before) flow:**

1. Client organisation uploads files to SharePoint  
2. Audit team downloads files from SharePoint to laptop  
3. Audit team uploads files to an Angular app  
4. Angular app saves files in a public ADLS account  
5. ADF copies from public ADLS to private ADLS  
6. Data available as External Volume in Databricks  
7. Run transformation and analytics processes  

**Slow, many manual steps, multiple systems.**

---

## The Solution

**Build this flow into their existing app** (the one that already transforms and visualises the data). Add: sign in with Microsoft → browse SharePoint in the app → copy straight into a volume. Then the same app transforms and visualises as today. No manual download, no ADLS step. One place for ingest, transform, and visualisation.

---

## At a Glance

| Before | After |
|--------|--------|
| Client → SharePoint → audit team download to laptop → upload to Angular app → public ADLS → ADF → private ADLS → External Volume → transform & analytics | **Existing app:** SharePoint (browse & copy in app) → volume → transform & analytics |
| Many manual steps; Angular, public/private ADLS, ADF | **One app** for ingest + transform + visualise |
