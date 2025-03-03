import React, { useMemo } from 'react';
import { useRiskStream } from '../hooks/useRiskStream';
import { VaRVisualizer } from './VaRVisualizer';
import { HeatmapLayer } from './HeatmapLayer';

interface RiskData {
  var95: number;
  liquidityMap: Map<string, number>;
  portfolioDelta: number;
}

/**
 * Real-time risk dashboard with streaming data visualization
 */
export const RiskDashboard: React.FC = () => {
  const { riskData, isLoading } = useRiskStream();

  const exposureScore = useMemo(() => 
    Math.sqrt(riskData.var95 ** 2 + riskData.portfolioDelta ** 2),
    [riskData]
  );

  return (
    <div className="grid grid-cols-3 gap-6 p-4 bg-dark-800">
      <VaRVisualizer 
        value={riskData.var95} 
        threshold={5e6} 
        severity={exposureScore > 7 ? 'critical' : 'normal'}
      />
      <HeatmapLayer 
        data={Array.from(riskData.liquidityMap.entries())}
        resolution="1m"
      />
      {isLoading && <div className="col-span-3 text-center">Loading...</div>}
    </div>
  );
};
