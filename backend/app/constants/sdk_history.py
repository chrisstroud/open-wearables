"""Server-owned history contracts for the dashboard.fitness iOS client."""

# This is the reviewed ``HealthDataRegistry.readTypes`` surface in the
# dashboard.fitness companion app.  Keep the server copy explicit: accepting a
# client-provided subset would let a manifest claim readiness while silently
# omitting a type that the product promises to import.
DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES: frozenset[str] = frozenset(
    {
        "HKCategoryTypeIdentifierSleepAnalysis",
        "HKQuantityTypeIdentifierActiveEnergyBurned",
        "HKQuantityTypeIdentifierBasalEnergyBurned",
        "HKQuantityTypeIdentifierBloodGlucose",
        "HKQuantityTypeIdentifierBodyFatPercentage",
        "HKQuantityTypeIdentifierBodyMass",
        "HKQuantityTypeIdentifierBodyMassIndex",
        "HKQuantityTypeIdentifierBodyTemperature",
        "HKQuantityTypeIdentifierDistanceCycling",
        "HKQuantityTypeIdentifierDistanceWalkingRunning",
        "HKQuantityTypeIdentifierFlightsClimbed",
        "HKQuantityTypeIdentifierHeartRate",
        "HKQuantityTypeIdentifierHeartRateVariabilitySDNN",
        "HKQuantityTypeIdentifierHeight",
        "HKQuantityTypeIdentifierInsulinDelivery",
        "HKQuantityTypeIdentifierLeanBodyMass",
        "HKQuantityTypeIdentifierOxygenSaturation",
        "HKQuantityTypeIdentifierRespiratoryRate",
        "HKQuantityTypeIdentifierRestingHeartRate",
        "HKQuantityTypeIdentifierSixMinuteWalkTestDistance",
        "HKQuantityTypeIdentifierStepCount",
        "HKQuantityTypeIdentifierVO2Max",
        "HKQuantityTypeIdentifierWaistCircumference",
        "HKQuantityTypeIdentifierWalkingAsymmetryPercentage",
        "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage",
        "HKQuantityTypeIdentifierWalkingSpeed",
        "HKQuantityTypeIdentifierWalkingStepLength",
        "HKWorkoutType",
    }
)

DASHBOARD_FITNESS_COVERAGE_POLICY_VERSION = "apple-health-v1"
