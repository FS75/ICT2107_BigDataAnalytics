import { Card, Title, Text, Tab, TabList, Grid } from "@tremor/react";
import React, { useState, useEffect } from "react";
import CompanyCard from "./CompanyCard";
import Donut from "./ReviewDonut";
import companyData from "../assets/test.json"

// yes we must format the sentiment in this way due to
// how donut chart takes in data

export default function PageShell() {
 
  const [selectedView, setSelectedView] = useState("1");
  return (
    <main>
      <Title>Dashboard</Title>
      <Text>A dashboard created for data analytics for ICT 2107.</Text>

      <TabList
        defaultValue="1"
        onValueChange={(value) => setSelectedView(value)}
        className="mt-6"
      >
        {
        /*
            add on as many tabs here, avoid dynamic rendering, it sucks 
            rmb to extend the ternary operator as well
        */
        }
        <Tab value="1" text="Company Information" />
        <Tab value="2" text="Sentiment Analysis" />
        <Tab value="3" text="Wordcloud" />
      </TabList>

      {selectedView === "1" ? (
        <>
        
            {/* Main section */}
            <Card className="mt-6">
                <CompanyCard
                    data = {companyData}
                />
            </Card>

            {/* KPI section */}
            <Grid numColsMd={2} className="mt-6 gap-6">
                <Card>
                <div className="h-28" />
                </Card>
                <Card>
                <div className="h-28" />
                </Card>
            </Grid>
        </>
      ) : selectedView === "2" ? (
        <Card className="mt-6">
            <Grid numColsSm={2} numColsLg={3} className="gap-6">
                {
                    
                    companyData.map((company) => (
                        <Donut
                            key={company.name}
                            data = {company}
                        /> 
                    ))
                }
            </Grid>
        </Card>
        
      ) : (
        <div className="mt-6">
          <Card>
            <div className="h-96">
                Throw some wordcloud in here
            </div>
          </Card>
        </div>
      )}
    </main>
  );
}