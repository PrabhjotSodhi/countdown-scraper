// Used by index.ts for creating and accessing items stored in supabaseDB

import * as dotenv from "dotenv";
dotenv.config();
dotenv.config({ path: `.env.local`, override: true });

import { createClient } from '@supabase/supabase-js';
import { logError, log, colour, validCategories } from "./utilities";
import { Product, UpsertResponse, ProductResponse } from "./typings";

let supabase;

export async function establishSupabase() {
  // Get CosmosDB connection string stored in .env
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_KEY;
  if (typeof supabaseUrl === 'string' && typeof supabaseKey === 'string') {
    supabase = createClient(supabaseUrl, supabaseKey);
  } else {
    throw new Error('Supabase URL or key is missing or not a string.');
  }
}

// upsertProductToSupabase()
// -------------------------
// Inserts or updates a product object to Supabase,
//  returns an UpsertResponse based on if and how the Product was updated

export async function upsertProductToSupabase(
  scrapedProduct: Product
): Promise<UpsertResponse> {
  try {
    // Check Supabase for any existing item using id and name as the partition key
    let { data: dbProduct, error } = await supabase
      .from('products')
      .select('*')
      .eq('id', scrapedProduct.id)
      .single();

    if (error && error.code !== 'PGRST116') {
        logError(error.message);
        return UpsertResponse.Failed;
    }

    // If an existing item was found in Supabase, check for update values before uploading
    if (dbProduct) {
      const response = buildUpdatedProduct(scrapedProduct, dbProduct);

      // Send updated product to Supabase
      let { data, error: upsertError } = await supabase
        .from('products')
        .upsert(response.product);

      if (upsertError) {
        logError(upsertError.message);
        return UpsertResponse.Failed;
      }
      return response.upsertType;
    }
    // If product with ID and exact name doesn't yet exist in Supabase
    else {
      scrapedProduct.currentPrice = scrapedProduct.currentPrice || 0;
      scrapedProduct.priceHistory = scrapedProduct.priceHistory || [];
      // If no existing product was found, create a new product
      let { data, error } = await supabase
        .from('products')
        .insert(scrapedProduct);

      if (error) {
        logError(error.message);
        return UpsertResponse.Failed;
      }

      console.log(
        `  New Product: ${scrapedProduct.name.slice(0, 47).padEnd(47)} - ${scrapedProduct.ingredients.length}`
      );

      return UpsertResponse.NewProduct;
    }
    /*else {
      // First check if there is an existing product with the same ID but different name(partition key)
      const querySpec = {
        query: `SELECT * FROM products p WHERE p.id = @id`,
        parameters: [
          {
            name: "@id",
            value: scrapedProduct.id,
          },
        ],
      };
      const { resources } = await container.items.query(querySpec).fetchAll();

      // If an existing ID was found, update the DB with the new name
      if (resources.length > 0) {
        // Cast existing product to correct type
        const dbProduct = resources[0] as Product;

        // Update product with new name
        const response = buildUpdatedProduct(scrapedProduct, dbProduct);
        response.product.name = scrapedProduct.name;

        // Send updated product to Supabase
        await container.items.upsert(response.product);
        return response.upsertType;
      } else {
        // If no existing ID was found, create a new product
        await container.items.create(scrapedProduct);

        console.log(
          `  New Product: ${scrapedProduct.name.slice(0, 47).padEnd(47)}` +
          ` | $ ${scrapedProduct.currentPrice}`
        );

        return UpsertResponse.NewProduct;
      }
    }
    // Manage any failed cosmos updates
    else if (cosmosResponse.statusCode === 409) {
      logError(`Conflicting ID found for product ${scrapedProduct.name}`);
      return UpsertResponse.Failed;
    } else {
      // If CosmoDB returns a status code other than 200 or 404, manage other errors here
      logError(`Supabase returned status code: ${cosmosResponse.statusCode}`);
      return UpsertResponse.Failed;
    }*/
  } catch (e: any) {
    logError(e.message);
    return UpsertResponse.Failed;
  }
}

// buildUpdatedProduct()
// ---------------------
// This takes a freshly scraped product and compares it with a found database product.
// It returns an updated product with data from both product versions

function buildUpdatedProduct(
  scrapedProduct: Product,
  dbProduct: Product
): ProductResponse {
  // Date objects pulled from Supabase need to re-parsed as strings in format yyyy-mm-dd
  let dbDay = dbProduct.lastUpdated.toString().slice(0, 10);
  let scrapedDay = scrapedProduct.lastUpdated.toISOString().slice(0, 10);

  // Measure the ingredients difference between the new scraped product and the old db product
  const ingredientsDifferent = !(
    dbProduct.ingredients.length === scrapedProduct.ingredients.length &&
    dbProduct.ingredients.every((value, index) => value === scrapedProduct.ingredients[index])
  );

  // If ingredients has changed
  if (ingredientsDifferent && scrapedProduct.ingredients.length > 1) {
    // Update the ingredient list of the old db product with the new scraped product ingredient list
    dbProduct.ingredients = scrapedProduct.ingredients;

    // Update everything but lastUpdated
    scrapedProduct.currentPrice = scrapedProduct.currentPrice || 0;
    scrapedProduct.priceHistory = scrapedProduct.priceHistory || [];
    scrapedProduct.lastUpdated = dbProduct.lastUpdated;

    // Return completed Product ready for uploading
    logIngredientChange(dbProduct, scrapedProduct.ingredients);
    return {
      upsertType: UpsertResponse.PriceChanged,
      product: scrapedProduct,
    };
  }

  // If any db categories are not included within the list of valid ones, update to scraped ones
  else if (
    !dbProduct.category.every((category) => {
      const isValid = validCategories.includes(category);
      return isValid;
    }) ||
    dbProduct.category === null
  ) {
    console.log(
      `  Categories Changed: ${scrapedProduct.name
        .padEnd(40)
        .substring(0, 40)}` +
      ` - ${dbProduct.category.join(" ")} > ${scrapedProduct.category.join(
        " "
      )}`
    );

    // Update everything but priceHistory and lastUpdated
    scrapedProduct.currentPrice = scrapedProduct.currentPrice || 0;
    scrapedProduct.priceHistory = scrapedProduct.priceHistory || [];
    scrapedProduct.lastUpdated = dbProduct.lastUpdated;

    // Return completed Product ready for uploading
    return {
      upsertType: UpsertResponse.InfoChanged,
      product: scrapedProduct,
    };
  }

  // Update other info
  else if (
    dbProduct.sourceSite !== scrapedProduct.sourceSite ||
    dbProduct.category.join(" ") !== scrapedProduct.category.join(" ") ||
    dbProduct.size !== scrapedProduct.size ||
    dbProduct.unitPrice !== scrapedProduct.unitPrice ||
    dbProduct.unitName !== scrapedProduct.unitName ||
    dbProduct.originalUnitQuantity !== scrapedProduct.originalUnitQuantity
  ) {
    // Update everything but priceHistory and lastUpdated
    scrapedProduct.lastUpdated = dbProduct.lastUpdated;

    // Return completed Product ready for uploading
    return {
      upsertType: UpsertResponse.InfoChanged,
      product: scrapedProduct,
    };
  } else {
    // Nothing has changed, only update lastChecked
    dbProduct.lastChecked = scrapedProduct.lastChecked;
    return {
      upsertType: UpsertResponse.AlreadyUpToDate,
      product: dbProduct,
    };
  }
}

// logIngredientChange()
// ----------------
// Log a per product ingredient change message,
//  coloured green for fewer ingredients, red for more ingredients

export function logIngredientChange(product: Product, newIngredients: string[]) {
  const ingredientsIncreased = newIngredients.length > product.ingredients.length;
  log(
    ingredientsIncreased ? colour.red : colour.green,
    "  Ingredients " +
    (ingredientsIncreased ? "Increased: " : "Decreased: ") +
    product.name.slice(0, 47).padEnd(47) +
    " | " +
    product.ingredients.join(", ").slice(0, 50).padEnd(50) +
    " > " +
    newIngredients.join(", ").slice(0, 50)
  );
}

// customQuery()
// -------------
// Function for running custom DB queries - used primarily for debugging
/*
export async function customQuery(): Promise<void> {
  const options: FeedOptions = {
    maxItemCount: 30,
  };
  const secondsDelayBetweenBatches = 5;
  const querySpec: SqlQuerySpec = {
    query: "SELECT * FROM products p",
  };

  log(colour.yellow, "Custom Query \n" + querySpec.query);

  const response = await container.items.query(querySpec, options);

  let batchCount = 0;
  const maxBatchCount = 900;
  let continueFetching = true;

  await (async () => {
    while (response.hasMoreResults() && continueFetching) {
      await delayedBatchFetch();
    }
  })();

  console.log("Custom Query Complete");
  return;

  function delayedBatchFetch() {
    return new Promise<void>((resolve) =>
      setTimeout(async () => {
        console.log(
          "Batch " +
          batchCount +
          " - Items [" +
          batchCount * options.maxItemCount! +
          " - " +
          (batchCount + 1) * options.maxItemCount!
        ) + "]";

        const batch = await response.fetchNext();
        const products = batch.resources as Product[];
        const items = batch.resources;

        products.forEach(async (p) => {
          let oldDatedPrice = 0;
          let requiresUpdate = false;

          p.priceHistory.forEach((datedPrice) => {
            let newDatedPrice = datedPrice.price;
            if (Math.abs(oldDatedPrice - newDatedPrice) < 0.04) {
              console.log(p.name);
              console.log(
                " - Tiny price difference detected on " +
                datedPrice.date.toDateString() +
                " - " +
                oldDatedPrice +
                " - " +
                newDatedPrice
              );
              datedPrice.price = 0;
              requiresUpdate = true;
            }
            oldDatedPrice = newDatedPrice;
          });

          if (requiresUpdate) {
            let updatedPriceHistory = p.priceHistory.filter((datedPrice) => {
              if (datedPrice.price > 0) return true;
              else return false;
            });

            console.log(
              " - Old price history length: " +
              p.priceHistory.length +
              " - new length: " +
              updatedPriceHistory.length
            );

            p.priceHistory = updatedPriceHistory;

            const uploadRes = await container.items.upsert(p);
            console.log(
              " - Uploaded updated product with status code: " +
              uploadRes.statusCode
            );
          }

          // item.name = item.name.replace('  ', ' ').trim();
          // let p: Product = item as Product;

          // const res = await container.item(item.id, item.name).delete();
          // console.log('delete ' + res.statusCode);

          // const uploadRes = await container.items.upsert(p);
          // console.log('upload ' + uploadRes.statusCode);
        });

        if (batchCount++ === maxBatchCount) continueFetching = false;

        resolve();
      }, secondsDelayBetweenBatches * 1000)
    );
  }
}
*/