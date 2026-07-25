import { test, expect } from "@playwright/test";

test.describe("Raft UI smoke tests", () => {
  test("page loads and shows cluster overview", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveTitle(/Raft/);
    await expect(page.locator("text=Cluster")).toBeVisible({ timeout: 10000 });
  });

  test("can submit a key-value write", async ({ page }) => {
    await page.goto("/");
    const keyInput = page.locator('input[placeholder*="key" i]');
    const valInput = page.locator('input[placeholder*="value" i]');
    const submitBtn = page.locator('button:has-text("Put")');

    await expect(keyInput).toBeVisible({ timeout: 10000 });
    await keyInput.fill("test-key");
    await valInput.fill("test-value");
    await submitBtn.click();

    const result = page.locator("text=test-key");
    await expect(result).toBeVisible({ timeout: 10000 });
  });

  test("cluster topology displays three nodes", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("text=leader").first()).toBeVisible({ timeout: 15000 });
    const nodes = page.locator('[data-testid="node-card"]');
    await expect(nodes).toHaveCount(3, { timeout: 15000 });
  });
});
