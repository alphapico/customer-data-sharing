namespace CustomerDataSharingLogic.ExternalConnections.CosmosDB
{
    public class IMSE5515Box : DefaultCosmosEntry
    {
        public IMSE5515Product[] products { get; set; }

        public IMSE5515Box()
        {
            product = "IMSE5515";
        }
    }

    public class IMSE5515Product
    {
        public string dmc { get; set; }
    }
}
