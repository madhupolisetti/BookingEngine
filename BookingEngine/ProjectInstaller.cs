using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.Threading.Tasks;

namespace BookingEngine
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();
        }
        protected override void OnBeforeInstall(IDictionary savedState)
        {
            this.SetServiceName();
            base.OnBeforeInstall(savedState);
        }
        protected override void OnBeforeUninstall(IDictionary savedState)
        {
            this.SetServiceName();
            base.OnBeforeUninstall(savedState);
        }
        private void SetServiceName()
        {
            if (Context.Parameters.ContainsKey("ServiceName"))
            {
                serviceInstaller1.ServiceName = Context.Parameters["ServiceName"];
            }

            if (Context.Parameters.ContainsKey("DisplayName"))
            {
                serviceInstaller1.DisplayName = Context.Parameters["DisplayName"];
            }
        }
    }
}
