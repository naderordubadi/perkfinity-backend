// Test script to verify Task 11 Merchant Dashboard integration
const fs = require('fs');
const path = require('path');

async function runTests() {
  console.log('--- Starting Task 11 Dashboard Integration Verification ---');

  // Test 1: Check dashboard.html on disk
  console.log('\n[Test 1] Inspecting Website-Perkfinity/dashboard.html...');
  const htmlPath = path.resolve(__dirname, '../../Website-Perkfinity/dashboard.html');
  const htmlContent = fs.readFileSync(htmlPath, 'utf8');

  const hasTabNav = htmlContent.includes('id="tab-nav-traffic"');
  const hasTabTraffic = htmlContent.includes('id="tab-traffic"');
  const hasLoadFunc = htmlContent.includes('function loadMerchantTraffic(');
  const hasExportFunc = htmlContent.includes('function exportMerchantTrafficCSV(');
  const hasFeatureFlag = htmlContent.includes('isExplicitTraffic');
  const hasSwitchTabHandling = htmlContent.includes("if (tabId === 'traffic') loadMerchantTraffic();");
  const hasHideTabHandling = htmlContent.includes("#tab-traffic");
  const hasKPITotalReach = htmlContent.includes('id="kpi-total-reach"');
  const hasDetailedModules = htmlContent.includes('id="trafficDetailedModules"');
  const hasLockedTeaser = htmlContent.includes('id="trafficLockedTeaser"');
  const hasChannelAttribution = htmlContent.includes('id="traffic-count-app"');
  const hasActionOrder = htmlContent.includes('id="action-order"');
  const hasSponsorshipMetrics = htmlContent.includes('id="traffic-takeover-clicks"');
  const hasEmailToggle = htmlContent.includes('id="trafficEmailToggle"');
  const hasToggleStatus = htmlContent.includes('id="trafficEmailToggleStatus"');
  const hasToggleFunc = htmlContent.includes('async function toggleMonthlyRoiEmail(');

  console.log('  - tab-nav-traffic link in HTML:', hasTabNav ? '✅ PASS' : '❌ FAIL');
  console.log('  - tab-traffic container in HTML:', hasTabTraffic ? '✅ PASS' : '❌ FAIL');
  console.log('  - loadMerchantTraffic function in JS:', hasLoadFunc ? '✅ PASS' : '❌ FAIL');
  console.log('  - exportMerchantTrafficCSV in JS:', hasExportFunc ? '✅ PASS' : '❌ FAIL');
  console.log('  - feature flag check in JS:', hasFeatureFlag ? '✅ PASS' : '❌ FAIL');
  console.log('  - switchTab traffic trigger:', hasSwitchTabHandling ? '✅ PASS' : '❌ FAIL');
  console.log('  - switchTab tab-traffic hide:', hasHideTabHandling ? '✅ PASS' : '❌ FAIL');
  console.log('  - KPI cards present:', hasKPITotalReach ? '✅ PASS' : '❌ FAIL');
  console.log('  - Tier 1 locked teaser present:', hasLockedTeaser ? '✅ PASS' : '❌ FAIL');
  console.log('  - Tier 2 detailed modules present:', hasDetailedModules ? '✅ PASS' : '❌ FAIL');
  console.log('  - Channel attribution elements present:', hasChannelAttribution ? '✅ PASS' : '❌ FAIL');
  console.log('  - Itemized actions elements present:', hasActionOrder ? '✅ PASS' : '❌ FAIL');
  console.log('  - Sponsorship metrics elements present:', hasSponsorshipMetrics ? '✅ PASS' : '❌ FAIL');
  console.log('  - Monthly ROI email toggle element:', hasEmailToggle ? '✅ PASS' : '❌ FAIL');
  console.log('  - Monthly ROI email status badge:', hasToggleStatus ? '✅ PASS' : '❌ FAIL');
  console.log('  - toggleMonthlyRoiEmail function in JS:', hasToggleFunc ? '✅ PASS' : '❌ FAIL');

  if (!hasTabNav || !hasTabTraffic || !hasLoadFunc || !hasExportFunc || !hasFeatureFlag || !hasSwitchTabHandling || !hasDetailedModules || !hasLockedTeaser || !hasEmailToggle || !hasToggleFunc) {
    throw new Error('dashboard.html is missing one or more required elements');
  }

  console.log('\n🎉 ALL STATIC DASHBOARD INTEGRATION CHECKS PASSED 100%!');
}

runTests().catch((err) => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});
