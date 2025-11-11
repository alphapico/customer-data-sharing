using CustomerDataSharingLogic.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CustomerDataSharingLogic.ExternalConnections.TMDB
{
    //used in TMDB PEN for Gaudi data
    public class RPA_FT_OSBE18_DMC //: BaseBusinessObject
    {
        public String DEVICE_DMC { get; set; }

        public static List<RPA_FT_OSBE18_DMC> GetExistingDMC(List<string> dmc)
        {
            var dataStatement = $@"SELECT DEVICE_DMC FROM TMDB_DATA.RPA_FT_OSBE18_DMC where DEVICE_DMC in ('{string.Join("', '", dmc)}')";
            var dataTable = TmdbDBConnection.GetData(dataStatement, null);
            var data = BaseDBHelper.GetList<RPA_FT_OSBE18_DMC>(dataTable);
            return data;
        }

        public static void CreateDmcNotExistingYet(List<RPA_FT_OSBE18_DMC> existingDMC, List<String> allDMC)
        {
            var existingDMCList = existingDMC.Select(d => d.DEVICE_DMC).ToList();
            var dmcToAdd = allDMC.ToList();
            dmcToAdd.RemoveAll(d => existingDMCList.Contains(d));
            TmdbDBConnection.InsertMassData(dmcToAdd.Select(d => new RPA_FT_OSBE18_DMC() { DEVICE_DMC = d }).ToList(), "TMDB_DATA.RPA_FT_OSBE18_DMC");
        }

        public static void DeleteDMC(List<RPA_FT_OSBE18_DMC> alreadyInDMCTable, List<string> dmcList)
        {
            var dmcToRemove = dmcList.ToList();
            dmcToRemove.RemoveAll(d => alreadyInDMCTable.Any(i => i.DEVICE_DMC == d));

            var dbStatement = $@"DELETE FROM TMDB_DATA.RPA_FT_OSBE18_DMC where DEVICE_DMC in ('{string.Join("', '", dmcToRemove)}')";
            TmdbDBConnection.Execute(dbStatement);
        }
    }
}
