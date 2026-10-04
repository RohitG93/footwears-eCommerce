import { ChangeDetectorRef, Component, inject, OnInit } from '@angular/core';
import { ShopService } from '../../core/services/shop.service';
import { Product } from '../../shared/Models/product';
import { ProductItemComponent } from "./product-item/product-item.component";
import { MatDialog } from '@angular/material/dialog';
import { FilterDialogComponent } from './filter-dialog/filter-dialog.component';
import { MatButton } from '@angular/material/button';
import { MatIcon } from '@angular/material/icon';
import { MatListOption, MatSelectionList, MatSelectionListChange } from '@angular/material/list';
import { MatMenu, MatMenuTrigger } from '@angular/material/menu';
import { ShortParams } from '../../shared/Models/shortParams';
import { MatPaginator } from '@angular/material/paginator';
import { Pagination } from '../../shared/Models/pagination';
import { FormsModule } from '@angular/forms';
import { MatFormField, MatLabel } from "@angular/material/select";
import { EmptyStateComponent } from '../../shared/components/empty-state/empty-state.component';

@Component({
  selector: 'app-shop',
  imports: [
    ProductItemComponent,
    MatButton,
    MatIcon,
    MatMenu,
    MatSelectionList,
    MatListOption,
    MatMenuTrigger,
    MatPaginator,
    FormsModule,
    EmptyStateComponent],
  templateUrl: './shop.component.html',
  styleUrl: './shop.component.scss',
})
export class ShopComponent implements OnInit {
  private shopService = inject(ShopService);
  private dialogService = inject(MatDialog);
  products?: Pagination<Product>;
  private cdr = inject(ChangeDetectorRef);
  selectedOptions= [
    {name: 'Alphabetical', value: 'name'},
    {name: 'Price: Low to High', value: 'price_asc'},
    {name: 'Price: High to Low', value: 'price_desc'}
  ];
  pageSizeOptions = [5, 10, 20, 25];

  shopParams= new ShortParams();

  ngOnInit(): void {
      this.ngInitialize();
  }

  ngInitialize(): void {
      this.shopService.getBrands();
      this.shopService.getTypes();

      this.getProductsData();
  }

  resetFilters(): void {
      this.shopParams = new ShortParams();
      this.getProductsData();
  }

  openFilterDialog(): void {
      const dialogRef = this.dialogService.open(FilterDialogComponent, {
          width: '500px',
          data: {
              selectedBrands: this.shopParams.brands,
              selectedTypes: this.shopParams.types
          }
      });

      dialogRef.afterClosed().subscribe(result => {
          if (result) {
              this.shopParams.brands = result.selectedBrands;
              this.shopParams.types = result.selectedTypes;
              this.shopParams.pageNumber = 1; // Reset to the first page when sort option changes 

              this.getProductsData();
          }
      });
    }

  applySortChange(event: MatSelectionListChange): void {
      const selectedSortOption = event.options[0].value;
      if (selectedSortOption !== this.shopParams.sort) {
          this.shopParams.sort = selectedSortOption;
          this.shopParams.pageNumber = 1; // Reset to the first page when sort option changes 
          console.log('Selected sort option:', this.shopParams.sort);

            this.getProductsData();
      }
  }

  onPageChange(event: any): void {
      this.shopParams.pageNumber = event.pageIndex + 1;
      this.shopParams.pageSize = event.pageSize;

      this.getProductsData();
  }

  onSearchChange(): void {
      this.shopParams.pageNumber = 1; // Reset to the first page when search changes
      console.log('Search term:', this.shopParams.search);
      this.getProductsData();
  }

  getProductsData(): void {
      this.shopService.getProducts(this.shopParams).subscribe({
          next: (response) => {
              this.products = response;
              this.cdr.detectChanges();
          },
          error: (error) => {
              console.error('Error fetching products:', error);
          }
      });
  }
}
  