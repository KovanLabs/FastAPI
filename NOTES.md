# Teaching notes

- The learner wants each construct tied to both **what it does** and **why it is used there**.
- Explain the active request flow before deep implementation details.
- Treat multi-line Python expressions as one logical statement, while accounting for every non-blank source line.
- When introducing parser internals, begin with a very simple real-world analogy, then map each part back to the exact code.
- The learner will write and modify implementation code. Guide one small step at a time, ask for their attempt, review it, and provide only focused corrections or minimal fragments if they remain blocked.
- Day 5 is attempt-first and contract-first: require a field cleaning policy and rejection catalogue before implementation, challenge inferred business meaning, and cite the exact official page/topic/full URL after each attempt.
- Day 5 exit evidence must include parameterized cleaning tests, cleaner idempotence, nested Pydantic error mapping, duplicate-scope tests, reconciled writer counts, duplicate-index memory measurement, and a frozen standard-Python golden result.
- Day 5 is paused at the missing/null/blank policy decision while the learner prepares a mentor review covering completed Day 1–4 work. Emphasize learned boundaries and measured evidence, not just a feature inventory.
- For the 12 October 2026 CEO review, the learner needs a business-first technical narrative: outcome, architectural flow, reasoned trade-offs, measured evidence, honest production boundaries, and a concise Q&A defense. Avoid file-by-file narration and unsupported “constant memory” or “handles any size” claims.
