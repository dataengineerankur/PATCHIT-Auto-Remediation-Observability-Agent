# PATCHIT Website Build Instructions

You are an expert front-end engineer + product designer + motion graphics specialist. Build a stunning, highly animated product website for **PATCHIT** — an AI-powered auto-remediation platform for data pipelines. The site must feel premium, intelligent, trustworthy, and visually demonstrate how the product works through animations.

---

## Product Identity

**Name:** PATCHIT  
**Full Form:** **P**ipeline **A**uto-**T**riaging, **C**ode **H**ealing & **I**ncident **T**ransformation  
**Tagline:** "Self-Healing Pipelines. Zero Midnight Pages."  
**Alternative Taglines:**
- "From Failure to Fix in Minutes, Not Hours"
- "Your AI Reliability Engineer That Never Sleeps"
- "Turn Pipeline Failures Into Pull Requests"

---

## Hard Rules

1. **Do NOT invent features** beyond what's described in this document.
2. **Heavy animation is REQUIRED** — this is a product demo site, not a corporate brochure.
3. **Show, don't tell** — use animated diagrams, flows, and live-feeling UI mockups.
4. **Dark theme mandatory** — premium tech product aesthetic (think Linear, Vercel, Raycast).
5. **Performance matters** — optimize animations, lazy load sections, use GPU-accelerated transforms.
6. **Mobile responsive** — animations should gracefully degrade on mobile.
7. **Respect prefers-reduced-motion** — provide static fallbacks.

---

## Tech Stack (Required)

- **Next.js 14+** (App Router) + TypeScript
- **TailwindCSS** with custom design tokens
- **Framer Motion** for scroll animations, page transitions, orchestrated reveals
- **Three.js or React Three Fiber** for 3D hero visualization (pipeline/agent flow)
- **Lottie** for icon animations and micro-interactions
- **Lucide React** for static icons
- **Optional:** GSAP for complex timeline animations

---

## Design Direction

### Color Palette
```css
--bg-primary: #0a0a0f;        /* Deep space black */
--bg-secondary: #12121a;      /* Card backgrounds */
--bg-tertiary: #1a1a25;       /* Elevated surfaces */
--accent-primary: #6366f1;    /* Indigo */
--accent-secondary: #8b5cf6;  /* Purple */
--accent-gradient: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #d946ef 100%);
--success: #10b981;           /* Green for "fixed" states */
--error: #ef4444;             /* Red for "failure" states */
--warning: #f59e0b;           /* Amber for "processing" */
--text-primary: #ffffff;
--text-secondary: #a1a1aa;
--text-muted: #71717a;
```

### Typography
- **Headlines:** Cal Sans, Satoshi, or Inter Display (bold, tight tracking)
- **Body:** Inter or Geist (clean, readable)
- **Code/Mono:** JetBrains Mono or Fira Code

### Visual Style
- Glassmorphism cards with subtle blur
- Glowing borders on interactive elements
- Particle/node animations in backgrounds
- Terminal/code aesthetics for technical sections
- Gradient mesh backgrounds (subtle, not overwhelming)

---

## Information Architecture

### Single-Page Sections (scroll-based)
1. **Hero** — Animated product intro + CTA
2. **Problem** — The pain of pipeline failures (with stats)
3. **Solution** — How PATCHIT works (animated flow)
4. **How It Works** — Multi-agent methodology deep dive
5. **Architecture** — Technical architecture diagram (animated)
6. **Integrations** — Airflow, Spark, dbt, n8n, Slack, etc.
7. **Features** — Key capabilities grid
8. **Live Demo** — Embedded or simulated demo
9. **Pricing/CTA** — Waitlist signup
10. **Footer** — Links, social, legal

### Additional Pages
- `/how-it-works` — Detailed agent methodology
- `/integrations` — Full integration catalog
- `/docs` — Documentation (can be placeholder)

---

## Section-by-Section Specifications

### 1. HERO SECTION

**Layout:** Full viewport, split or centered
**Background:** Animated 3D visualization of a data pipeline with flowing particles representing data, and AI agents (glowing orbs) moving between pipeline stages

**Content:**
```
[Animated Badge] "Now in Private Beta"

# Self-Healing Data Pipelines
## Powered by Multi-Agent AI

PATCHIT detects pipeline failures, diagnoses root causes, 
proposes fixes, and creates pull requests — automatically.

[Primary CTA: "Join Waitlist"] [Secondary CTA: "See Demo"]

[Animated Stats Row]
- "< 5 min" Mean Time to Fix
- "85%" Auto-Resolution Rate  
- "Zero" Midnight Pages
```

**Animations:**
1. **Hero text:** Staggered fade-up with blur-to-sharp transition
2. **3D Pipeline:** Continuous subtle rotation, particles flowing through nodes
3. **Stats:** Count-up animation on scroll into view
4. **CTAs:** Glow pulse on hover, magnetic cursor effect
5. **Background:** Gradient mesh slowly shifts colors

---

### 2. PROBLEM SECTION

**Layout:** Split — left text, right animated visualization

**Content:**
```
# The $2.5M Problem

Every year, data teams lose thousands of hours to:

[Animated Cards - appear on scroll]
├── 🔥 Midnight Alerts — Engineers woken for issues that could self-heal
├── 🔄 Repetitive Fixes — Same failure patterns, same manual patches
├── 📊 Slow Recovery — Hours to diagnose what AI can solve in minutes
├── 💸 Lost Revenue — Delayed reports, stale dashboards, broken ML models
└── 😰 Burnout — On-call fatigue draining your best engineers

[Animated Stat]
"73% of pipeline failures are caused by just 12 recurring patterns"
```

**Visualization:** Animated timeline showing:
1. Failure occurs (red pulse)
2. Alert fires (notification animation)
3. Engineer wakes up (person icon)
4. Hours of debugging (clock spinning)
5. Manual fix deployed (slow)
6. Repeat cycle (loop animation)

---

### 3. SOLUTION SECTION

**Layout:** Centered with large animated diagram

**Content:**
```
# Meet PATCHIT

Your AI Reliability Engineer that transforms failures 
into fixes while you sleep.

[Animated Before/After Comparison]

WITHOUT PATCHIT:
Failure → Alert → Wake Engineer → Debug → Fix → Test → Deploy
         └──────────── 4-6 hours ────────────┘

WITH PATCHIT:
Failure → AI Detects → AI Diagnoses → AI Fixes → PR Created → You Approve
         └──────────── 3-5 minutes ────────────┘
```

**Animation:** Side-by-side timeline that animates:
- Left side (without): Slow, frustrating, red-tinted
- Right side (with): Fast, smooth, green-tinted
- Visual emphasis on the 50x speed difference

---

### 4. HOW IT WORKS — MULTI-AGENT METHODOLOGY

**This is the most important animated section.** Show the complete agent pipeline.

**Layout:** Horizontal scroll or vertical timeline with animated stages

**Content:**
```
# The PATCHIT Agent Pipeline

A coordinated team of specialized AI agents, 
each with a focused mission.

[Stage 1: DETECTION]
Icon: 👁️ Eye/Radar
Title: "Failure Detection"
Description: "Continuous polling of Airflow, Spark, dbt. 
Instant detection when any task fails."
Animation: Radar sweep, failure appears as red blip

[Stage 2: INGESTION]
Icon: 📥 Inbox
Title: "Event Ingestion"
Description: "Structured failure events with logs, 
stack traces, and artifact URIs."
Animation: Data streams flowing into a central hub

[Stage 3: RCA AGENT]
Icon: 🔍 Magnifying Glass
Title: "Root Cause Analysis"
Description: "Multi-variant prompting with scoring. 
Traces the true source — producer vs consumer vs infra."
Tech Detail: "Uses meta-prompting, negative prompting, 
and automatic variant scoring."
Animation: AI analyzing code, highlighting the root cause

[Stage 4: FIX AGENT]
Icon: 🔧 Wrench
Title: "Fix Proposal"
Description: "Generates minimal, safe code patches. 
Follows strict control flow rules."
Tech Detail: "Engineered prompts prevent unreachable code, 
ensure proper raise/return handling."
Animation: Code diff appearing line by line

[Stage 5: VALIDATION]
Icon: ✅ Checkmark
Title: "Sandbox Verification"
Description: "Applies patch in isolated sandbox. 
Runs pytest, compileall, and custom verification."
Tech Detail: "Supports Spark sandbox testing, 
dbt parse/build, API integration tests."
Animation: Green checkmarks appearing as tests pass

[Stage 6: POLICY ENGINE]
Icon: 🛡️ Shield
Title: "Safety Guardrails"
Description: "Policy engine validates: 
allowed paths, file limits, no secrets."
Animation: Shield checking items, blocking dangerous changes

[Stage 7: PR CREATION]
Icon: 🚀 Rocket
Title: "Pull Request"
Description: "Creates GitHub PR with full context: 
RCA, evidence pack, verification results."
Animation: PR appearing in GitHub-style UI mockup

[Stage 8: INTEGRATIONS]
Icon: 🔔 Bell
Title: "Notify & Track"
Description: "n8n workflows fan out to Slack, 
ServiceNow, PagerDuty, Datadog."
Animation: Notifications spreading to multiple channels
```

**Master Animation:** 
An animated "agent" (glowing orb or robot character) travels through each stage, leaving a trail. Each stage lights up as the agent passes through. The whole flow should feel like a well-orchestrated assembly line.

---

### 5. ARCHITECTURE SECTION

**Layout:** Full-width animated architecture diagram

**Content:**
```
# Technical Architecture

Production-grade. Pipeline-agnostic. Multi-repo ready.

[Animated Diagram showing:]

┌─────────────────────────────────────────────────────────────┐
│                    DATA PLATFORMS                            │
│  [Airflow] ←→ [Spark/Databricks] ←→ [dbt] ←→ [Custom]      │
└───────────────────────┬─────────────────────────────────────┘
                        │ Failure Events
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                      PATCHIT CORE                            │
│                                                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ Poller   │→ │ Ingester │→ │ Analyzer │→ │ Fixer    │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │
│        │              │             │            │          │
│        ▼              ▼             ▼            ▼          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              PROMPT ORCHESTRATOR                     │   │
│  │   Meta Prompts │ Variants │ Scoring │ Memory        │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Sandbox  │  │ Policy   │  │ Evidence │  │ Memory   │   │
│  │ Verifier │  │ Engine   │  │ Packs    │  │ Store    │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└───────────────────────┬─────────────────────────────────────┘
                        │ Actions
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                     INTEGRATIONS                             │
│  [GitHub PRs] [Slack] [ServiceNow] [PagerDuty] [n8n]       │
└─────────────────────────────────────────────────────────────┘
```

**Animation:** 
- Nodes light up sequentially showing data flow
- Particles/pulses travel along connection lines
- Hovering on a component shows tooltip with details

---

### 6. FEATURES GRID

**Layout:** Responsive grid of feature cards

**Features:**
```json
[
  {
    "icon": "🔄",
    "title": "Multi-Repo Support",
    "description": "Route incidents to the correct repository based on pipeline ID or stack trace file paths. Supports monorepos and multi-repo architectures.",
    "highlight": true
  },
  {
    "icon": "🧪",
    "title": "Sandbox Verification",
    "description": "Every fix is tested in isolation before PR creation. Runs pytest, compileall, dbt parse/build, and custom commands."
  },
  {
    "icon": "⚡",
    "title": "Spark Integration",
    "description": "First-class Spark support. Local spark-submit validation, executor log analysis, and Databricks/EMR compatibility."
  },
  {
    "icon": "🛡️",
    "title": "Policy Engine",
    "description": "Configurable guardrails: allowed file paths, max files touched, forbidden patterns. Blocks dangerous changes automatically."
  },
  {
    "icon": "📝",
    "title": "Evidence Packs",
    "description": "Every remediation includes complete audit trail: logs, diffs, RCA, verification results. Full compliance-ready documentation."
  },
  {
    "icon": "🔌",
    "title": "n8n Workflows",
    "description": "Pre-built workflow templates for Slack, ServiceNow, PagerDuty, and Datadog. Enrich events with Spark logs before remediation."
  },
  {
    "icon": "🧠",
    "title": "Multi-Agent Prompting",
    "description": "Engineered prompt pipeline with meta-prompts, negative rules, variant generation, and automatic scoring for best fix selection."
  },
  {
    "icon": "🔍",
    "title": "Smart Repo Selection",
    "description": "Stack-aware routing identifies the correct repository from error traces, even when pipeline IDs are generic."
  },
  {
    "icon": "🔁",
    "title": "Duplicate PR Guard",
    "description": "Memory-backed deduplication prevents PR spam. Links to existing PRs instead of creating redundant fixes."
  },
  {
    "icon": "📊",
    "title": "Real-Time UI",
    "description": "Animated dashboard showing live agent activity. SSE-powered event stream with game-style agent visualization."
  },
  {
    "icon": "🎯",
    "title": "Control Flow Rules",
    "description": "Smart prompting prevents unreachable code fixes. Never adds code after raise/return statements."
  },
  {
    "icon": "🔐",
    "title": "Secrets Safety",
    "description": "Never touches .env files, credentials, or tokens. Automatic detection and refusal of sensitive changes."
  }
]
```

**Animation:** Cards appear with staggered fade-up, subtle hover lift effect, icon animations on hover

---

### 7. INTEGRATIONS SECTION

**Layout:** Logo grid with hover details

**Integrations:**
```
PIPELINE PLATFORMS:
- Apache Airflow (built-in poller)
- Apache Spark / PySpark
- Databricks (Delta Lake, Unity Catalog)
- dbt (parse/build verification)
- AWS Glue

AI PROVIDERS:
- OpenRouter (GPT-4, Claude, etc.)
- Groq (fast inference)
- Cursor Cloud Agents

VERSION CONTROL:
- GitHub (PR creation, branch management)
- GitLab (coming soon)

NOTIFICATIONS:
- Slack
- Microsoft Teams
- PagerDuty
- Datadog

WORKFLOW AUTOMATION:
- n8n (pre-built templates)
- Webhook API (any system)

INCIDENT MANAGEMENT:
- ServiceNow
- Jira (coming soon)
```

**Animation:** Logos in a flowing marquee or interactive grid, connections animate when hovering

---

### 8. LIVE DEMO / SIMULATION

**Layout:** Full-width simulated terminal/UI

**Content:** An animated simulation showing:
1. Airflow DAG fails (red indicator)
2. PATCHIT detects (notification pulse)
3. Logs analyzed (scrolling terminal)
4. RCA generated (text appearing)
5. Fix proposed (code diff animation)
6. Sandbox passes (green checkmarks)
7. PR created (GitHub-style card)

**This should feel like watching a real remediation in action.**

**Optional:** Embed actual PATCHIT UI iframe or video

---

### 9. PRICING / WAITLIST CTA

**Layout:** Centered with gradient background

**Content:**
```
# Ready to Stop Fighting Fires?

Join the private beta and let PATCHIT handle your pipeline failures.

[Large Input: Email address]
[CTA Button: "Join Waitlist"]

✓ No credit card required
✓ First 100 users get lifetime discount
✓ Self-hosted option available
```

**Animation:** Subtle floating particles, input field glow on focus, button pulse

---

### 10. FOOTER

**Content:**
```
[Logo: PATCHIT]
Pipeline Auto-Triaging, Code Healing & Incident Transformation

PRODUCT          RESOURCES        COMPANY
Features         Docs             About
Integrations     Blog             Contact
Pricing          Changelog        Careers

© 2026 PATCHIT. All rights reserved.

[Social Icons: GitHub, Twitter/X, LinkedIn, Discord]
```

---

## Animation Specifications

### Page Load Sequence
1. Logo/brand fade in (0.3s)
2. Hero headline stagger (0.5s delay, 0.1s between words)
3. Hero subheadline fade up (0.3s after headline)
4. CTAs slide up (0.2s after subheadline)
5. 3D background begins animating

### Scroll Animations
- **Section headers:** Fade up + blur-to-sharp
- **Cards:** Staggered entrance (0.1s delay between)
- **Diagrams:** Progressive reveal as user scrolls
- **Stats:** Count-up animation

### Micro-interactions
- **Buttons:** Scale 1.02 + glow on hover
- **Cards:** Lift + border glow on hover
- **Links:** Underline animation
- **Icons:** Subtle bounce or pulse on hover

### 3D Hero Visualization
- **Scene:** Network of connected nodes (pipeline stages)
- **Particles:** Data points flowing between nodes
- **Agents:** Glowing orbs moving through the pipeline
- **Camera:** Slow orbit or parallax on mouse move
- **Performance:** Use instancing, limit particle count, GPU-accelerated

---

## Technical Implementation Notes

### Performance Targets
- Lighthouse Performance: > 90
- First Contentful Paint: < 1.5s
- Largest Contentful Paint: < 2.5s
- Total Blocking Time: < 200ms

### Code Organization
```
src/
├── app/
│   ├── page.tsx              # Main landing page
│   ├── how-it-works/
│   ├── integrations/
│   └── layout.tsx
├── components/
│   ├── Hero/
│   ├── Problem/
│   ├── Solution/
│   ├── HowItWorks/
│   ├── Architecture/
│   ├── Features/
│   ├── Integrations/
│   ├── Demo/
│   ├── Pricing/
│   ├── Footer/
│   └── shared/
├── lib/
│   ├── animations.ts
│   └── utils.ts
├── content/
│   └── product.ts            # All product content
└── styles/
    └── globals.css
```

### Content Data Structure
```typescript
// src/content/product.ts
export const product = {
  name: "PATCHIT",
  fullForm: "Pipeline Auto-Triaging, Code Healing & Incident Transformation",
  tagline: "Self-Healing Pipelines. Zero Midnight Pages.",
  description: "PATCHIT detects pipeline failures, diagnoses root causes, proposes fixes, and creates pull requests — automatically.",
  
  stats: [
    { value: "< 5 min", label: "Mean Time to Fix", icon: "clock" },
    { value: "85%", label: "Auto-Resolution Rate", icon: "check" },
    { value: "Zero", label: "Midnight Pages", icon: "moon" },
  ],
  
  agentPipeline: [
    {
      id: "detection",
      icon: "radar",
      title: "Failure Detection",
      description: "Continuous polling of Airflow, Spark, dbt...",
      techDetail: "Built-in poller with configurable intervals",
    },
    // ... all 8 stages
  ],
  
  features: [
    // ... 12 features
  ],
  
  integrations: {
    platforms: ["Airflow", "Spark", "dbt", ...],
    aiProviders: ["OpenRouter", "Groq", "Cursor"],
    notifications: ["Slack", "PagerDuty", ...],
    // ...
  },
};
```

---

## Deliverables

1. **Complete Next.js repository** with all components
2. **README.md** with setup and deployment instructions
3. **Vercel-ready** configuration
4. **All animations** implemented and performant
5. **Mobile responsive** design
6. **Dark theme** with proper contrast
7. **SEO optimized** with meta tags and OpenGraph

---

## Reference Inspiration

- [Linear.app](https://linear.app) — Clean product animation
- [Vercel.com](https://vercel.com) — Developer-focused design
- [Raycast.com](https://raycast.com) — Premium dark theme
- [Stripe.com](https://stripe.com) — Animated diagrams
- [Clerk.com](https://clerk.com) — Product visualization

---

## Final Notes

This website should make someone watching it think: "This is exactly what I need for my data team." The animations should **explain the product** — not just look pretty. Every motion should have purpose: showing data flow, agent progression, or before/after comparison.

The goal is a website that:
1. **Educates** — Explains what PATCHIT does in seconds
2. **Impresses** — Feels premium and intelligent
3. **Converts** — Drives waitlist signups
4. **Differentiates** — Stands out from generic SaaS sites

Build this to be the best product website you've ever created.

