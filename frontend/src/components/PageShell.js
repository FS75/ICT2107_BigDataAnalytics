import { Card, Title, Text, Tab, TabList, Grid } from "@tremor/react";

import { useState } from "react";

export default function PageShell() {
  const [selectedView, setSelectedView] = useState("1");
  const [numViews, updateNumViews] = useState(3)
  return (
    <>
      <main>
        <Title>Dashboard</Title>
        <Text>A dashboard to visualise data crawled for ICT 2107</Text>

        <TabList
          defaultValue="1"
          onValueChange={(value) => setSelectedView(value)}
          className="mt-6"
        >
         
          
          <Tab value="1" text="Page 1" />
          <Tab value="2" text="Page 2" />
          <Tab value="3" text="Page 3" />
        </TabList>

        {selectedView === "1" ? (
          <>
            <Grid numColsMd={2} numColsLg={3} className="gap-6 mt-6">
              <Card>
                {/* Placeholder to set height */}
                <div className="h-28" />
              </Card>
              <Card>
                {/* Placeholder to set height */}
                <div className="h-28" />
              </Card>
              <Card>
                {/* Placeholder to set height */}
                <div className="h-28" />
              </Card>
            </Grid>

            <div className="mt-6">
              <Card>
                <div className="h-80" />
              </Card>
            </div>
          </>
        ) : selectedView === "2" ? (
          <div className="mt-6">
            <Card>
              <div className="h-96" />
            </Card>
          </div>
        ) : (
          <div className="mt-6">
            <Card>
              <div className="h-96">
                hello i am in page { selectedView }
              </div>
            </Card>
          </div>
        ) }
      </main>
    </>
  );
}
