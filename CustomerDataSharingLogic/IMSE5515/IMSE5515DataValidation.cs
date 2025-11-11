using CustomerDataSharingLogic.ExternalConnections.TMDB;
using osram.OSAS.Logging;
using osram.OSAS.Reflection;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CustomerDataSharingLogic.IMSE5515
{
    public static class IMSE5515DataValidation
    {
        private static List<DataColumnValidation> validations = new List<DataColumnValidation>()
        {
            new DataColumnValidation("CX", "BLUE", 20, 0.13, 0.17),
            new DataColumnValidation("CX", "BLUE", 50, 0.10, 0.18),
            new DataColumnValidation("CX", "GREEN", 20, 0.11, 0.29),
            new DataColumnValidation("CX", "GREEN", 50, 0.09, 0.29),
            new DataColumnValidation("CX", "RED", 20, 0.67, 0.73),
            new DataColumnValidation("CX", "RED", 50, 0.67, 0.73),

            new DataColumnValidation("CY", "BLUE", 20, 0.015, 0.12),
            new DataColumnValidation("CY", "BLUE", 50, 0.005, 0.12),
            new DataColumnValidation("CY", "GREEN", 20, 0.68, 0.8),
            new DataColumnValidation("CY", "GREEN", 50, 0.6, 0.8),
            new DataColumnValidation("CY", "RED", 20, 0.28, 0.33),
            new DataColumnValidation("CY", "RED", 50, 0.28, 0.33),

            new DataColumnValidation("IV", "BLUE", 20, 0.2, 1.0),
            new DataColumnValidation("IV", "BLUE", 50, 0.3, 2.0),
            new DataColumnValidation("IV", "GREEN", 20, 1.8, 4.0),
            new DataColumnValidation("IV", "GREEN", 50, 2.5, 8.0),
            new DataColumnValidation("IV", "RED", 20, 0.5, 2.5),
            new DataColumnValidation("IV", "RED", 50, 1.0, 5.0),

            new DataColumnValidation("UF", "BLUE", 20, 2.4, 3.3),
            new DataColumnValidation("UF", "BLUE", 50, 2.8, 3.7),
            new DataColumnValidation("UF", "GREEN", 20, 2.0, 3.1),
            new DataColumnValidation("UF", "GREEN", 50, 2.5, 3.6),
            new DataColumnValidation("UF", "RED", 20, 1.5, 2.4),
            new DataColumnValidation("UF", "RED", 50, 2.0, 2.8),
        };

        public static bool ValidateData(List<REP_D_VJ_REEL_DATALOG> measurementData, List<REP_D_VJ_REEL_DATALOG> measurementDataOfAllOperations, out List<string> errors, bool amountValidation = true)
        {
            var validationResult = new List<string>();

            var positions = measurementData.Select(m => m.REEL_SEQ_POS).Distinct().ToList();
            foreach(var position in positions)
            {
                var dmcOfPos = measurementData.Where(m => m.REEL_SEQ_POS == position).Select(m => m.DMC).Distinct().ToList();
                if(dmcOfPos.Count > 1)
                    validationResult.Add($"position {position} has multiple DMC in TMDB: "+ string.Join(", ", dmcOfPos));
            }
            if (positions.Min() < 1)
                validationResult.Add($"min position on reel is < 1!");
            if (positions.Max() > 2000)
                validationResult.Add($"max position on reel is > 2000 ({positions.Max()})!");

            if (amountValidation)
            {
                ////validate amount -> should be 2000 on a reel
                //if (measurementData.Count < 2000 * 0.99 || measurementData.Count > 2000 * 1.01)
                //    validationResult.Add($"invalid amount of DMC: 2000 +- 1% expected, {measurementData.Count} found");
                if (measurementData.Count != 2000)
                {
                    var amountByOperation = measurementDataOfAllOperations.Select(m => m.OPERATION_NUMBER).Distinct().ToDictionary(m => m, m => measurementDataOfAllOperations.Count(m2 => m2.OPERATION_NUMBER == m));
                    var amountByOperationStr = String.Join(", ", amountByOperation.Select(m => $"{m.Key}: {m.Value}"));
                    var missing = Enumerable.Range(1, 2000).Except(measurementData.Select(m => m.REEL_SEQ_POS)).ToList();
                    var error = $"invalid amount of DMC: 2000 expected, {measurementData.Count} found in operation {measurementData.First().OPERATION_NUMBER}";
                    if (missing.Count < 20)
                        error += $", position(s) {String.Join(", ", missing)} missing";
                    error += $" - ({amountByOperationStr})";
                    validationResult.Add(error);
                }
            }

            //check that all dmc have 10 characters
            var dmcWithDifferentCharThan10 = measurementData.Select(m => m.DMC).Where(d => d.Length != 10).ToList();
            if(dmcWithDifferentCharThan10.Count > 0)
                validationResult.Add($"invalid DMC length - 10 char expected, found different length on {String.Join(", ", dmcWithDifferentCharThan10)}");

            //check	Forbidden character(O,V,G,Q) on DMC name
            var dmcWithInvalidChar = measurementData.Select(m => m.DMC).Where(d => 
                d.IndexOf("O", StringComparison.OrdinalIgnoreCase) >= 0 || 
                d.IndexOf("V", StringComparison.OrdinalIgnoreCase) >= 0 || 
                d.IndexOf("G", StringComparison.OrdinalIgnoreCase) >= 0 || 
                d.IndexOf("Q", StringComparison.OrdinalIgnoreCase) >= 0).ToList();
            if (dmcWithInvalidChar.Count > 0)
                validationResult.Add($"DMC with invalid characters found (O,V,G,Q): {String.Join(", ", dmcWithInvalidChar)}");

            //validate that the DMC is unique
            int distinctAmount = measurementData.Select(m => m.DMC).Distinct().Count();
            if (measurementData.Count != distinctAmount)
                validationResult.Add($"DMC is not unique");

            //validate that the positions are unique
            int distinctAmountOfPositions = measurementData.Select(m => m.REEL_SEQ_POS).Distinct().Count();
            if (measurementData.Count != distinctAmountOfPositions)
                validationResult.Add($"Reel Positions are not unique");

            foreach (var measurement in measurementData)
            {
                foreach (var validation in validations)
                {
                    try
                    {
                        var propertyName = $"{validation.Parameter}_{validation.Color}_{validation.Current}";
                        double value = (double)ReflectionHelper.GetPropertyValue(measurement, propertyName);

                        var result = ValidateEntry(value, validation, measurement.DMC);
                        if(result != null)
                            validationResult.Add(result);
                    }
                    catch (Exception ex)
                    {
                        validationResult.Add($"Error validation {validation.Parameter} for column {validation.Color} and current {validation.Current} for DMC {measurement.DMC}: " + ex.Message);
                    }
                }
            }

            validationResult.RemoveAll(e => e is null);
            errors = validationResult;

            if (validationResult.Count == 0)
                return true;
            else
            {
                LogHelper.Info(typeof(IMSE5515DataValidation), "Validation failed: " + measurementData.First().REEL_ID + Environment.NewLine + String.Join(Environment.NewLine, validationResult));
                return false;
            }
        }

        private static String ValidateEntry(double value, DataColumnValidation validation, string dmc)
        {
            if (value < validation.MinAllowedValue)
                return $"value '{value}' is lower than allowed (parameter {validation.Parameter}), DMC {dmc}";
            else if (value > validation.MaxAllowedValue)
                return $"value '{value}' is higher than allowed (parameter {validation.Parameter}), DMC {dmc}";
            return null;
        }
    }

    public class DataColumnValidation
    {
        public string Parameter { get; set; }
        public string Color { get; set; }
        public int Current { get; set; }
        public double MinAllowedValue { get; set; }
        public double MaxAllowedValue { get; set; }

        public DataColumnValidation(string parameter, string color, int current, double minAllowedValue, double maxAllowedValue)
        {
            Parameter = parameter;
            Color = color;
            Current = current;
            MinAllowedValue = minAllowedValue;
            MaxAllowedValue = maxAllowedValue;
        }
    }
}