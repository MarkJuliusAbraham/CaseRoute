export function canStartRun({ generatorLive, runKnown, activeRun, starting, resetting }) {
    return generatorLive && runKnown && !activeRun && !starting && !resetting;
}
